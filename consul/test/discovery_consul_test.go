package test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	cfacade "github.com/cherry-game/cherry/facade"
	cprofile "github.com/cherry-game/cherry/profile"
	cherryConsul "github.com/cherry-game/components/consul"
	consulapi "github.com/hashicorp/consul/api"
)

type mockApplication struct {
	nodeID     string
	nodeType   string
	rpcAddress string
	dieChan    chan bool
}

func newMockApplication(nodeID, nodeType, rpcAddress string) *mockApplication {
	return &mockApplication{
		nodeID:     nodeID,
		nodeType:   nodeType,
		rpcAddress: rpcAddress,
		dieChan:    make(chan bool, 1),
	}
}

func (m *mockApplication) NodeID() string {
	return m.nodeID
}

func (m *mockApplication) NodeType() string {
	return m.nodeType
}

func (m *mockApplication) Address() string {
	return ""
}

func (m *mockApplication) RpcAddress() string {
	return m.rpcAddress
}

func (m *mockApplication) Settings() cfacade.ProfileJSON {
	return nil
}

func (m *mockApplication) Enabled() bool {
	return true
}

func (m *mockApplication) Running() bool {
	return true
}

func (m *mockApplication) DieChan() chan bool {
	return m.dieChan
}

func (m *mockApplication) IsFrontend() bool {
	return false
}

func (m *mockApplication) Register(components ...cfacade.IComponent) {
}

func (m *mockApplication) Find(name string) cfacade.IComponent {
	return nil
}

func (m *mockApplication) Remove(name string) cfacade.IComponent {
	return nil
}

func (m *mockApplication) All() []cfacade.IComponent {
	return nil
}

func (m *mockApplication) OnShutdown(fn ...func()) {
}

func (m *mockApplication) Startup() {
}

func (m *mockApplication) Shutdown() {
}

func (m *mockApplication) Serializer() cfacade.ISerializer {
	return nil
}

func (m *mockApplication) Discovery() cfacade.IDiscovery {
	return nil
}

func (m *mockApplication) Cluster() cfacade.ICluster {
	return nil
}

func (m *mockApplication) ActorSystem() cfacade.IActorSystem {
	return nil
}

func TestConsulDiscovery_RegisterAndDeregister(t *testing.T) {
	address := "127.0.0.1:8500"
	cli, err := consulapi.NewClient(&consulapi.Config{Address: address})
	if err != nil {
		t.Fatalf("create consul client failed: %v", err)
	}

	if _, err = cli.Agent().Self(); err != nil {
		t.Skipf("consul is unavailable at %s: %v", address, err)
	}

	nodeID := fmt.Sprintf("consul-test-%d", time.Now().UnixNano())
	prefix := fmt.Sprintf("cherry-test-%d", time.Now().UnixNano())
	rpcAddress := "127.0.0.1:19090"
	nodeType := "gateway"

	profilePath, err := createProfileFile(nodeID, nodeType, rpcAddress, address, prefix)
	if err != nil {
		t.Fatalf("create profile file failed: %v", err)
	}

	if _, err = cprofile.Init(profilePath, nodeID); err != nil {
		t.Fatalf("init profile failed: %v", err)
	}

	app := newMockApplication(nodeID, nodeType, rpcAddress)
	discovery := cherryConsul.New()
	discovery.Load(app)
	defer stopDiscovery(discovery, app)

	if err = waitUntil(5*time.Second, func() (bool, error) {
		svc, _, err := cli.Agent().Service(nodeID, nil)
		if err != nil {
			return false, err
		}
		return svc != nil, nil
	}); err != nil {
		t.Fatalf("service register check failed: %v", err)
	}

	stopDiscovery(discovery, app)

	if err = waitUntil(5*time.Second, func() (bool, error) {
		svc, _, err := cli.Agent().Service(nodeID, nil)
		if err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "unknown service id") {
				return true, nil
			}
			return false, err
		}
		return svc == nil, nil
	}); err != nil {
		t.Fatalf("service deregister check failed: %v", err)
	}
}

func TestConsulDiscovery_RegisterAndDeregister_CommunicationModes(t *testing.T) {
	address := "127.0.0.1:8500"
	cli, err := consulapi.NewClient(&consulapi.Config{Address: address})
	if err != nil {
		t.Fatalf("create consul client failed: %v", err)
	}

	if _, err = cli.Agent().Self(); err != nil {
		t.Skipf("consul is unavailable at %s: %v", address, err)
	}

	modes := []string{"http", "dns", "grpc", "invalid-mode"}
	for _, mode := range modes {
		t.Run(mode, func(t *testing.T) {
			nodeID := fmt.Sprintf("consul-test-%s-%d", mode, time.Now().UnixNano())
			prefix := fmt.Sprintf("cherry-test-%s-%d", mode, time.Now().UnixNano())
			rpcAddress := "127.0.0.1:19091"
			nodeType := "gateway"

			profilePath, profileErr := createProfileFileWithCommunication(nodeID, nodeType, rpcAddress, address, prefix, mode)
			if profileErr != nil {
				t.Fatalf("create profile file failed: %v", profileErr)
			}

			if _, profileErr = cprofile.Init(profilePath, nodeID); profileErr != nil {
				t.Fatalf("init profile failed: %v", profileErr)
			}

			app := newMockApplication(nodeID, nodeType, rpcAddress)
			discovery := cherryConsul.New()
			discovery.Load(app)
			defer stopDiscovery(discovery, app)

			if profileErr = waitUntil(5*time.Second, func() (bool, error) {
				svc, _, serviceErr := cli.Agent().Service(nodeID, nil)
				if serviceErr != nil {
					return false, serviceErr
				}
				return svc != nil, nil
			}); profileErr != nil {
				t.Fatalf("service register check failed: %v", profileErr)
			}

			stopDiscovery(discovery, app)

			if profileErr = waitUntil(5*time.Second, func() (bool, error) {
				svc, _, serviceErr := cli.Agent().Service(nodeID, nil)
				if serviceErr != nil {
					if strings.Contains(strings.ToLower(serviceErr.Error()), "unknown service id") {
						return true, nil
					}
					return false, serviceErr
				}
				return svc == nil, nil
			}); profileErr != nil {
				t.Fatalf("service deregister check failed: %v", profileErr)
			}
		})
	}
}

func createProfileFile(nodeID, nodeType, rpcAddress, consulAddress, prefix string) (string, error) {
	return createProfileFileWithCommunication(nodeID, nodeType, rpcAddress, consulAddress, prefix, "http")
}

func createProfileFileWithCommunication(nodeID, nodeType, rpcAddress, consulAddress, prefix, communication string) (string, error) {
	dir, err := os.MkdirTemp("", "consul-profile-*")
	if err != nil {
		return "", err
	}

	content := fmt.Sprintf(`{
  "env": "test",
  "debug": true,
  "print_level": "debug",
  "node": {
    "%s": [
      {
        "node_id": "%s",
        "address": "127.0.0.1:18080",
        "rpc_address": "%s",
        "enabled": true,
        "__settings__": {}
      }
    ]
  },
  "cluster": {
    "consul": {
      "address": "%s",
      "ttl": 3,
      "prefix": "%s",
      "communication": "%s",
      "dns_address": "127.0.0.1:8600",
      "grpc_address": "127.0.0.1:8502"
    }
  }
}`, nodeType, nodeID, rpcAddress, consulAddress, prefix, communication)

	filePath := filepath.Join(dir, "profile-test.json")
	if err = os.WriteFile(filePath, []byte(content), 0o644); err != nil {
		return "", err
	}

	return filePath, nil
}

func waitUntil(timeout time.Duration, condition func() (bool, error)) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		done, err := condition()
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("condition not satisfied within %s", timeout)
}

func stopDiscovery(discovery *cherryConsul.Consul, app *mockApplication) {
	discovery.OnStop()
	select {
	case app.DieChan() <- true:
	default:
	}
}
