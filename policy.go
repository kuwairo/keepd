package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
)

type Plan struct {
	Recursive bool
	Keep      struct {
		Frequent *uint
		Hourly   *uint
		Daily    *uint
		Weekly   *uint
		Monthly  *uint
	}
}

type Policy struct {
	Prefix    string
	LocalTime bool
	Targets   map[string]Plan
	Groups    map[string]struct {
		Members []string
		Plan    Plan
	}
}

func LoadPolicy(path string) (*Policy, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("cannot open policy file: %w", err)
	}
	defer f.Close()

	dec := json.NewDecoder(f)
	dec.DisallowUnknownFields()

	var p Policy
	if err := dec.Decode(&p); err != nil {
		return nil, fmt.Errorf("cannot parse policy file: %w", err)
	}

	if p.Prefix == "" {
		return nil, errors.New("prefix is not specified")
	}
	for _, r := range p.Prefix {
		if r < 'a' || r > 'z' {
			return nil, errors.New("prefix contains forbidden characters (not a-z)")
		}
	}

	if p.Targets == nil {
		p.Targets = make(map[string]Plan)
	}
	for name, g := range p.Groups {
		for _, m := range g.Members {
			if _, ok := p.Targets[m]; ok {
				return nil, fmt.Errorf("group %q contains previously specified target %q", name, m)
			}
			p.Targets[m] = g.Plan
		}
	}
	clear(p.Groups)

	return &p, nil
}

func (p *Policy) ExtractPools() []string {
	m := make(map[string]struct{})
	for t := range p.Targets {
		parts := strings.Split(t, "/")
		m[parts[0]] = struct{}{}
	}

	pools := make([]string, 0, len(m))
	for p := range m {
		pools = append(pools, p)
	}

	return pools
}
