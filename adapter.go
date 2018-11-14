package consuladapter

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/casbin/casbin/model"
	"github.com/hashicorp/consul/api"
	"github.com/micro/go-config/source"
	"net"
	"fmt"
)

var (
	DefaultPrefix = "/micro/config/"
)

type Adapter struct {
	prefix      string
	stripPrefix string
	addr        string
	opts        source.Options
	client      *api.Client
}

// NewDBAdapter is the constructor for Adapter.
func NewAdapter(opts ...source.Option) *Adapter {
	return newAdapter(opts...)
}

func newAdapter(opts ...source.Option) *Adapter {
	options := source.NewOptions(opts...)

	// use default config
	config := api.DefaultConfig()

	// check if there are any addrs
	a, ok := options.Context.Value(addressKey{}).(string)
	if ok {
		addr, port, err := net.SplitHostPort(a)
		if ae, ok := err.(*net.AddrError); ok && ae.Err == "missing port in address" {
			port = "8500"
			addr = a
			config.Address = fmt.Sprintf("%s:%s", addr, port)
		} else if err == nil {
			config.Address = fmt.Sprintf("%s:%s", addr, port)
		}
	}

	// create the client
	client, _ := api.NewClient(config)

	prefix := DefaultPrefix
	sp := ""
	f, ok := options.Context.Value(prefixKey{}).(string)
	if ok {
		prefix = f
	}

	if b, ok := options.Context.Value(stripPrefixKey{}).(bool); ok && b {
		sp = prefix
	}

	return &Adapter{
		prefix:      prefix,
		stripPrefix: sp,
		addr:        config.Address,
		opts:        options,
		client:      client,
	}
}

func loadPolicyKey(line string, model model.Model) {
	if line == "" {
		return
	}

	tokens := strings.Split(line, ";")
	key := tokens[0]
	sec := key[:1]
	model[sec][key].Policy = append(model[sec][key].Policy, tokens[1:])

}

// LoadPolicy loads policy from consul.
func (a *Adapter) LoadPolicy(model model.Model) error {
	line := [][]string{}

	pair, _, err := a.client.KV().Get("rp", nil)
	if err != nil {
		return err
	}
	if pair != nil {
		json.Unmarshal(pair.Value, &line)

		for _, v := range line {
			if len(v) > 2 {
				v = append([]string{"p"}, v...)
			} else {
				v = append([]string{"g"}, v...)
			}

			rule := strings.Join(v, ";")
			loadPolicyKey(rule, model)

		}

	}

	return nil
}

func (a *Adapter) writePolicyKey(rule [][]string) error {
	pair, _, err := a.client.KV().Get("rp", nil)
	if err != nil {
		return err
	}

	value, _ := json.Marshal(rule)

	p := &api.KVPair{Key: "rp", Value: []byte(value)}

	//If not set, the default value is 0, and CAS will fail
	if pair != nil {
		p.ModifyIndex = pair.ModifyIndex
	}

	if success, _, err := a.client.KV().CAS(p, nil); success {
		if err != nil {
			return err
		}
	} else {
		err = errors.New("Check and set returned false for Consul KV")
		return err
	}
	return nil
}

// SavePolicy saves policy to consul.
func (a *Adapter) SavePolicy(model model.Model) error {

	var rule [][]string
	if len(model["p"]["p"].Policy) != 0 {
		rule = append(model["p"]["p"].Policy, rule...)

	}
	if len(model["g"]["g"].Policy) != 0 {
		rule = append(model["g"]["g"].Policy, rule...)
	}

	err := a.writePolicyKey(rule)
	if err != nil {
		return err
	}

	return nil
}

// AddPolicy adds a policy rule to the storage.
func (a *Adapter) AddPolicy(sec string, ptype string, rule []string) error {
	return errors.New("not implemented")
}

// RemovePolicy removes a policy rule from the storage.
func (a *Adapter) RemovePolicy(sec string, ptype string, rule []string) error {
	return errors.New("not implemented")
}

// RemoveFilteredPolicy removes policy rules that match the filter from the storage.
func (a *Adapter) RemoveFilteredPolicy(sec string, ptype string, fieldIndex int, fieldValues ...string) error {
	return errors.New("not implemented")
}