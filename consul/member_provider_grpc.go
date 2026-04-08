package consul

import (
	"context"
	"fmt"

	cproto "github.com/cherry-game/cherry/net/proto"
	"github.com/hashicorp/consul/proto-public/pbdns"
	"github.com/miekg/dns"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type grpcMemberProvider struct {
	prefix string
	domain string
	conn   *grpc.ClientConn
	cli    pbdns.DNSServiceClient
}

func newGRPCMemberProvider(prefix, domain, grpcAddress string) (*grpcMemberProvider, error) {
	conn, err := grpc.NewClient(grpcAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	return &grpcMemberProvider{
		prefix: prefix,
		domain: domain,
		conn:   conn,
		cli:    pbdns.NewDNSServiceClient(conn),
	}, nil
}

func (p *grpcMemberProvider) ListMembers(ctx context.Context) ([]*cproto.Member, error) {
	queryMsg := new(dns.Msg)
	queryMsg.SetQuestion(dns.Fqdn(fmt.Sprintf("%s.%s", p.prefix, p.domain)), dns.TypeSRV)
	msg, err := queryMsg.Pack()
	if err != nil {
		return nil, err
	}

	resp, err := p.cli.Query(ctx, &pbdns.QueryRequest{
		Msg:      msg,
		Protocol: pbdns.Protocol_PROTOCOL_UDP,
	})
	if err != nil {
		return nil, err
	}

	dnsResp := new(dns.Msg)
	if err = dnsResp.Unpack(resp.GetMsg()); err != nil {
		return nil, err
	}

	return parseDNSMembers(dnsResp), nil
}

func (p *grpcMemberProvider) Close() error {
	return p.conn.Close()
}
