package consul

import (
	"time"

	"github.com/hashicorp/consul/api"
)

const (
	defaultAddress         = "127.0.0.1:8500"
	defaultTTL             = 5 * time.Second
	defaultDeregisterAfter = 30 * time.Second
	defaultWatchWaitTime   = 10 * time.Second
	defaultWatchRetryDelay = 2 * time.Second
)

type Options struct {
	Address    string
	Scheme     string
	Token      string
	Datacenter string

	TTL             time.Duration
	DeregisterAfter time.Duration
}

func DefaultOptions() Options {
	return normalizeOptions(Options{})
}

func toConsulConfig(options Options) *api.Config {
	consulCfg := api.DefaultConfig()
	if options.Address != "" {
		consulCfg.Address = options.Address
	}
	if options.Scheme != "" {
		consulCfg.Scheme = options.Scheme
	}
	if options.Token != "" {
		consulCfg.Token = options.Token
	}
	if options.Datacenter != "" {
		consulCfg.Datacenter = options.Datacenter
	}
	return consulCfg
}

func normalizeOptions(options Options) Options {
	if options.Address == "" {
		options.Address = defaultAddress
	}
	if options.TTL <= time.Nanosecond*2 {
		options.TTL = defaultTTL
	}
	if options.DeregisterAfter <= 0 {
		options.DeregisterAfter = defaultDeregisterAfter
	}
	return options
}
