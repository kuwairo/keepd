package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"
)

const (
	PrefixMinLength   = 1
	PrefixMaxLength   = 32
	DefaultPrefix     = "keepd"
	DefaultPolicyPath = "./policy.json" // TODO: change
)

type Policy struct {
	Targets map[string]Plan `json:"targets"`
}

type Plan struct {
	Keep struct {
		Frequent uint `json:"frequent,omitempty"`
		Hourly   uint `json:"hourly,omitempty"`
		Daily    uint `json:"daily,omitempty"`
		Weekly   uint `json:"weekly,omitempty"`
		Monthly  uint `json:"monthly,omitempty"`
	} `json:"keep"`
}

// TODO: keepd.2006-01-02.15:04:05.frequent

func main() {
	prefix := flag.String("n", DefaultPrefix, "prefix for snapshot names")
	policyPath := flag.String("p", DefaultPolicyPath, "path to the policy file")
	flag.Parse()

	switch {
	case len(*prefix) < PrefixMinLength:
		log.Fatalln("prefix length is too short")
	case len(*prefix) > PrefixMaxLength:
		log.Fatalln("prefix length is too long")
	}

	for _, r := range *prefix {
		if r < 'a' || r > 'z' {
			log.Fatalln("prefix contains non-alphabetic characters (not a-z)")
		}
	}

	f, err := os.Open(*policyPath)
	if err != nil {
		log.Fatalf("cannot open policy file: %s\n", err)
	}
	defer f.Close()

	dec := json.NewDecoder(f)
	dec.DisallowUnknownFields()

	var policy Policy
	if err := dec.Decode(&policy); err != nil {
		log.Fatalf("cannot parse policy file: %s\n", err)
	}

	log.Printf("%+v\n", policy)
}
