package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Plan struct {
	Keep struct {
		Frequent *uint
		Hourly   *uint
		Daily    *uint
		Weekly   *uint
		Monthly  *uint
	}
}

type Policy struct {
	Prefix  string
	Targets map[string]Plan
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

type Service struct {
	policy      *Policy
	pools       []string
	regexpByTag map[string]*regexp.Regexp
	// TODO: inject event logger
}

func NewService(policy *Policy) *Service {
	// TODO: this shouldn't be part of service initialization
	t := reflect.TypeOf(Plan{}.Keep)
	m := make(map[string]*regexp.Regexp, t.NumField())
	base := `\.\d{4}-\d{2}-\d{2}\.\d{2}:\d{2}:\d{2}\.`
	for i := 0; i < t.NumField(); i++ {
		n := strings.ToLower(t.Field(i).Name)
		m[n] = regexp.MustCompile("(?m)" + policy.Prefix + base + n + "$")
	}

	return &Service{
		policy:      policy,
		pools:       policy.ExtractPools(),
		regexpByTag: m,
	}
}

func (s *Service) Enforce(keepFn func(Plan) (string, *uint)) {
	for t, p := range s.policy.Targets {
		tag, keep := keepFn(p)
		if keep == nil {
			continue
		}

		log.Printf("enforcing %q (keep %d) for target %q\n", tag, *keep, t)

		if *keep > 0 {
			if err := CreateSnapshot(t, s.policy.Prefix, tag); err != nil {
				log.Printf("cannot snapshot target %q: %s\n", t, err)
			}
		}

		names, err := ListSnapshotNames(t, s.regexpByTag[tag])
		if err != nil {
			log.Printf("cannot list snapshots of target %q: %s\n", t, err)
			continue
		}
		if len(names) <= int(*keep) {
			continue
		}

		for _, n := range names[int(*keep):] {
			if err := DestroySnapshot(t, string(n)); err != nil {
				log.Printf("cannot destroy snapshot \"%s@%s\": %s\n", t, n, err)
			}
		}
	}
}

func (s *Service) FrequentJob() {
	s.Enforce(func(p Plan) (string, *uint) {
		return "frequent", p.Keep.Frequent
	})
}

func (s *Service) RegularJob(tick time.Time) {
	s.FrequentJob()
	s.Enforce(func(p Plan) (string, *uint) {
		return "hourly", p.Keep.Hourly
	})

	weekYear, week := tick.ISOWeek()
	year, month, yearDay := tick.Year(), tick.Month(), tick.YearDay()

	jobsByTag := map[string]*struct {
		LastRunTimestamp int64
		TriggerFn        func(int64) bool
	}{
		"daily": {0, func(lrt int64) bool {
			t := time.Unix(lrt, 0)
			dayChanged := year != t.Year() || yearDay != t.YearDay()
			if dayChanged {
				s.Enforce(func(p Plan) (string, *uint) {
					return "daily", p.Keep.Daily
				})
			}
			return dayChanged
		}},
		"weekly": {0, func(lrt int64) bool {
			tWeekYear, tWeek := time.Unix(lrt, 0).ISOWeek()
			weekChanged := weekYear != tWeekYear || week != tWeek
			if weekChanged {
				s.Enforce(func(p Plan) (string, *uint) {
					return "weekly", p.Keep.Weekly
				})
			}
			return weekChanged
		}},
		"monthly": {0, func(lrt int64) bool {
			t := time.Unix(lrt, 0)
			monthChanged := year != t.Year() || month != t.Month()
			if monthChanged {
				s.Enforce(func(p Plan) (string, *uint) {
					return "monthly", p.Keep.Monthly
				})
			}
			return monthChanged
		}},
	}

	keyFormat := "org.keepd:last%sjob"

	for _, p := range s.pools {
		for t, job := range jobsByTag {
			key := fmt.Sprintf(keyFormat, t)
			value, err := GetPoolProperty(p, key)
			if err != nil {
				switch {
				case errors.Is(err, ErrInvalidProperty):
				case errors.Is(err, ErrPoolNotFound):
					log.Printf("cannot access pool %q: %s\n", p, err)
				default:
					log.Printf("cannot get property %q of pool %q: %s\n", key, p, err)
				}
				continue
			}

			unixTime, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				log.Printf("ignoring invalid timestamp %q (property %q of pool %q)\n", value, key, p)
				continue
			}
			if unixTime > job.LastRunTimestamp {
				job.LastRunTimestamp = unixTime
			}
		}
	}

	tickValue := strconv.FormatInt(tick.Unix(), 10)
	for t, job := range jobsByTag {
		if jobRan := job.TriggerFn(job.LastRunTimestamp); !jobRan {
			continue
		}

		key := fmt.Sprintf(keyFormat, t)
		for _, p := range s.pools {
			if err := SetPoolProperty(p, key, tickValue); err != nil {
				log.Printf("cannot set property %q of pool %q: %s\n", key, p, err)
			}
		}
	}
}

// TODO: handle signals
func main() {
	policyPath := flag.String("p", "", "path to the policy file")
	flag.Parse()

	if *policyPath == "" {
		log.Fatalln("path to the policy file is not specified")
	}

	policy, err := LoadPolicy(*policyPath)
	if err != nil {
		log.Fatalf("cannot load the policy: %s\n", err)
	}

	service := NewService(policy)

	var wg sync.WaitGroup
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-interrupt:
			log.Println("(!) waiting for jobs to finish")
			wg.Wait()
			log.Println("(!) exiting")
			return
		case t := <-ticker.C:
			switch t.Minute() {
			case 0:
				wg.Add(1)
				go func() { // TODO: does `t` need to be a parameter?
					defer wg.Done()
					service.RegularJob(t)
				}()
			case 15, 30, 45:
				wg.Add(1)
				go func() {
					defer wg.Done()
					service.FrequentJob()
				}()
			}
		}
	}
}
