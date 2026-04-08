package consul

import (
	"context"

	cproto "github.com/cherry-game/cherry/net/proto"
	consulapi "github.com/hashicorp/consul/api"
	jsoniter "github.com/json-iterator/go"
)

type httpMemberProvider struct {
	cli    *consulapi.Client
	prefix string
}

func (p *httpMemberProvider) ListMembers(_ context.Context) ([]*cproto.Member, error) {
	entries, _, err := p.cli.Health().Service(p.prefix, "", true, nil)
	if err != nil {
		return nil, err
	}

	members := make([]*cproto.Member, 0, len(entries))
	for _, entry := range entries {
		memberStr, ok := entry.Service.Meta["member"]
		if !ok {
			continue
		}

		member := &cproto.Member{}
		if err = jsoniter.UnmarshalFromString(memberStr, member); err != nil {
			continue
		}
		members = append(members, member)
	}

	return members, nil
}

func (p *httpMemberProvider) Close() error {
	return nil
}
