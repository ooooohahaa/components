package consul

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"

	cproto "github.com/cherry-game/cherry/net/proto"
	"github.com/miekg/dns"
)

type dnsMemberProvider struct {
	prefix     string
	domain     string
	dnsAddress string
}

func (p *dnsMemberProvider) ListMembers(_ context.Context) ([]*cproto.Member, error) {
	resp, err := queryDNSByUDP(p.prefix, p.domain, p.dnsAddress)
	if err != nil {
		return nil, err
	}

	return parseDNSMembers(resp), nil
}

func (p *dnsMemberProvider) Close() error {
	return nil
}

func queryDNSByUDP(prefix, domain, dnsAddress string) (*dns.Msg, error) {
	queryMsg := new(dns.Msg)
	queryMsg.SetQuestion(dns.Fqdn(fmt.Sprintf("%s.%s", prefix, domain)), dns.TypeSRV)
	dnsClient := &dns.Client{Net: "udp"}
	resp, _, err := dnsClient.Exchange(queryMsg, dnsAddress)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func parseDNSMembers(resp *dns.Msg) []*cproto.Member {
	if resp == nil {
		return nil
	}

	addressMap := make(map[string]string, len(resp.Extra))
	for _, extra := range resp.Extra {
		switch r := extra.(type) {
		case *dns.A:
			addressMap[dns.Fqdn(r.Hdr.Name)] = r.A.String()
		case *dns.AAAA:
			addressMap[dns.Fqdn(r.Hdr.Name)] = r.AAAA.String()
		}
	}

	members := make([]*cproto.Member, 0, len(resp.Answer))
	for _, answer := range resp.Answer {
		srv, ok := answer.(*dns.SRV)
		if !ok {
			continue
		}

		target := dns.Fqdn(srv.Target)
		host := strings.TrimSuffix(target, ".")
		if addr, found := addressMap[target]; found && addr != "" {
			host = addr
		}

		address := net.JoinHostPort(host, strconv.Itoa(int(srv.Port)))
		members = append(members, &cproto.Member{
			NodeID:   address,
			NodeType: "",
			Address:  address,
			Settings: make(map[string]string),
		})
	}

	return members
}
