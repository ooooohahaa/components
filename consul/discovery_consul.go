package consul

import (
	"fmt"
	"net"
	"strconv"
	"time"

	cfacade "github.com/cherry-game/cherry/facade"
	clog "github.com/cherry-game/cherry/logger"
	cdiscovery "github.com/cherry-game/cherry/net/discovery"
	cproto "github.com/cherry-game/cherry/net/proto"
	cprofile "github.com/cherry-game/cherry/profile"
	consulapi "github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/api/watch"
	jsoniter "github.com/json-iterator/go"
)

// Consul consul方式发现服务
type Consul struct {
	app cfacade.IApplication
	cdiscovery.DiscoveryDefault
	prefix string
	config *consulapi.Config
	ttl    int64
	cli    *consulapi.Client // consul client
}

func New() *Consul {
	return &Consul{}
}

func (p *Consul) Name() string {
	return "consul"
}

func (p *Consul) Load(app cfacade.IApplication) {
	p.DiscoveryDefault.PreInit()
	p.app = app
	p.ttl = 10

	clusterConfig := cprofile.GetConfig("cluster").GetConfig(p.Name())
	if clusterConfig.LastError() != nil {
		clog.Fatalf("consul config not found. err = %v", clusterConfig.LastError())
		return
	}

	p.loadConfig(clusterConfig)
	p.init()
	p.register()
	p.watch()

	clog.Infof("[consul] init complete! [address = %v]", p.config.Address)
}

func (p *Consul) OnStop() {
	serviceID := p.app.NodeID()
	err := p.cli.Agent().ServiceDeregister(serviceID)
	clog.Infof("consul stopping! deregister service err = %v", err)
}

func (p *Consul) loadConfig(config cfacade.ProfileJSON) {
	p.config = consulapi.DefaultConfig()

	address := config.GetString("address", "127.0.0.1:8500")
	p.config.Address = address

	if token := config.GetString("token"); token != "" {
		p.config.Token = token
	}

	p.ttl = config.GetInt64("ttl", 5)
	p.prefix = config.GetString("prefix", "cherry")
}

func (p *Consul) init() {
	var err error
	p.cli, err = consulapi.NewClient(p.config)
	if err != nil {
		clog.Fatalf("consul connect fail. err = %v", err)
		return
	}
}

func (p *Consul) register() {
	registerMember := &cproto.Member{
		NodeID:   p.app.NodeID(),
		NodeType: p.app.NodeType(),
		Address:  p.app.RpcAddress(),
		Settings: make(map[string]string),
	}

	jsonString, err := jsoniter.MarshalToString(registerMember)
	if err != nil {
		clog.Fatal(err)
		return
	}

	// parse address to host and port
	host, portStr, err := net.SplitHostPort(p.app.RpcAddress())
	if err != nil {
		host = p.app.RpcAddress()
		portStr = "0"
	}
	port, _ := strconv.Atoi(portStr)

	registration := &consulapi.AgentServiceRegistration{
		ID:      p.app.NodeID(),
		Name:    p.prefix,
		Tags:    []string{p.app.NodeType()},
		Port:    port,
		Address: host,
		Meta: map[string]string{
			"member": jsonString,
		},
		Check: &consulapi.AgentServiceCheck{
			TTL:                            fmt.Sprintf("%ds", p.ttl),
			DeregisterCriticalServiceAfter: "1m",
		},
	}

	err = p.cli.Agent().ServiceRegister(registration)
	if err != nil {
		clog.Fatal(err)
		return
	}

	// 启动 TTL 续约机制
	p.keepAlive()
}

func (p *Consul) keepAlive() {
	checkID := "service:" + p.app.NodeID()
	// TTL check requires updating at most half the TTL interval
	ticker := time.NewTicker(time.Duration(p.ttl*1000/2) * time.Millisecond)

	go func() {
		// 初始调用一次
		if err := p.cli.Agent().PassTTL(checkID, "TTL OK"); err != nil {
			clog.Warnf("consul keepalive error: %v", err)
		}

		for {
			select {
			case <-ticker.C:
				if err := p.cli.Agent().PassTTL(checkID, "TTL OK"); err != nil {
					clog.Warnf("consul keepalive error: %v", err)
				}
			case die := <-p.app.DieChan():
				if die {
					ticker.Stop()
					return
				}
			}
		}
	}()
}

func (p *Consul) watch() {
	plan, err := watch.Parse(map[string]interface{}{
		"type":    "service",
		"service": p.prefix,
	})
	if err != nil {
		clog.Fatalf("consul watch parse error: %v", err)
		return
	}

	plan.Handler = func(idx uint64, data interface{}) {
		entries, ok := data.([]*consulapi.ServiceEntry)
		if !ok {
			return
		}

		currentNodes := make(map[string]bool)

		for _, entry := range entries {
			// filter by health check
			passing := true
			for _, check := range entry.Checks {
				if check.Status != consulapi.HealthPassing {
					passing = false
					break
				}
			}
			if !passing {
				continue
			}

			memberStr, ok := entry.Service.Meta["member"]
			if !ok {
				continue
			}

			member := &cproto.Member{}
			err := jsoniter.UnmarshalFromString(memberStr, member)
			if err != nil {
				continue
			}

			currentNodes[member.NodeID] = true

			if _, found := p.GetMember(member.NodeID); !found {
				p.AddMember(member)
			}
		}

		// Remove stale nodes
		for nodeID := range p.Map() {
			if !currentNodes[nodeID] {
				p.RemoveMember(nodeID)
			}
		}
	}

	go func() {
		if err := plan.Run(p.config.Address); err != nil {
			clog.Fatalf("consul watch run error: %v", err)
		}
	}()
}
