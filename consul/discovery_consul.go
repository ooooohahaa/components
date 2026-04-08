package consul

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	cfacade "github.com/cherry-game/cherry/facade"
	clog "github.com/cherry-game/cherry/logger"
	cdiscovery "github.com/cherry-game/cherry/net/discovery"
	cproto "github.com/cherry-game/cherry/net/proto"
	cprofile "github.com/cherry-game/cherry/profile"
	consulapi "github.com/hashicorp/consul/api"
	jsoniter "github.com/json-iterator/go"
)

const (
	communicationHTTP = "http"
	communicationGRPC = "grpc"
	communicationDNS  = "dns"
)

type serviceMemberProvider interface {
	ListMembers(ctx context.Context) ([]*cproto.Member, error)
	Close() error
}

func getAddressWithPort(address string, port int) string {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		host = address
	}

	return net.JoinHostPort(host, strconv.Itoa(port))
}

// Consul consul方式发现服务
type Consul struct {
	app cfacade.IApplication
	cdiscovery.DiscoveryDefault
	prefix         string
	config         *consulapi.Config
	ttl            int64
	communication  string
	watchInterval  time.Duration
	dnsDomain      string
	dnsAddress     string
	grpcAddress    string
	cli            *consulapi.Client
	memberProvider serviceMemberProvider
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

	if p.memberProvider != nil {
		if closeErr := p.memberProvider.Close(); closeErr != nil {
			clog.Warnf("consul provider close error: %v", closeErr)
		}
	}
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
	p.communication = strings.ToLower(config.GetString("communication", communicationHTTP))
	p.watchInterval = time.Duration(config.GetInt64("watch_interval_ms", 1000)) * time.Millisecond
	if p.watchInterval < 200*time.Millisecond {
		p.watchInterval = 200 * time.Millisecond
	}
	p.dnsDomain = config.GetString("dns_domain", "service.consul")
	p.dnsAddress = config.GetString("dns_address", getAddressWithPort(p.config.Address, 8600))
	p.grpcAddress = config.GetString("grpc_address", getAddressWithPort(p.config.Address, 8502))
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
	var err error
	p.memberProvider, err = p.newMemberProvider()
	if err != nil {
		clog.Fatalf("consul member provider init error: %v", err)
		return
	}

	p.syncMembers()
	ticker := time.NewTicker(p.watchInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				p.syncMembers()
			case die := <-p.app.DieChan():
				if die {
					return
				}
			}
		}
	}()
}

func (p *Consul) newMemberProvider() (serviceMemberProvider, error) {
	switch p.communication {
	case communicationGRPC:
		return newGRPCMemberProvider(p.prefix, p.dnsDomain, p.grpcAddress)
	case communicationDNS:
		return &dnsMemberProvider{
			prefix:     p.prefix,
			domain:     p.dnsDomain,
			dnsAddress: p.dnsAddress,
		}, nil
	default:
		p.communication = communicationHTTP
		return &httpMemberProvider{
			cli:    p.cli,
			prefix: p.prefix,
		}, nil
	}
}

func (p *Consul) syncMembers() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	members, err := p.memberProvider.ListMembers(ctx)
	if err != nil {
		clog.Warnf("consul sync members error: %v", err)
		return
	}

	currentNodes := make(map[string]bool, len(members))
	for _, member := range members {
		if member == nil || member.NodeID == "" {
			continue
		}

		currentNodes[member.NodeID] = true
		if _, found := p.GetMember(member.NodeID); !found {
			p.AddMember(member)
		}
	}

	for nodeID := range p.Map() {
		if !currentNodes[nodeID] {
			p.RemoveMember(nodeID)
		}
	}
}
