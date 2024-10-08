package main

import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type EventType int

const (
	ETCreateSnapshot EventType = iota
	ETListSnapshotNames
	ETDestroySnapshot
	ETGetPoolProperty
	ETSetPoolProperty
)

type Event struct {
	Type      EventType
	Success   bool
	Timestamp time.Time
	Target    string
	Job       string
	Recursive bool
	Reason    string
}

type Journal interface {
	Add(event Event) error
}

type SnapshotMatcher map[string]*regexp.Regexp

func NewSnapshotMatcher(prefix string) SnapshotMatcher {
	t := reflect.TypeOf(Plan{}.Keep)
	regexpByTag := make(map[string]*regexp.Regexp, t.NumField())

	dateTimePattern := `\.\d{4}-\d{2}-\d{2}\.\d{2}:\d{2}:\d{2}\.`

	for i := 0; i < t.NumField(); i++ {
		tag := strings.ToLower(t.Field(i).Name)
		regexpByTag[tag] = regexp.MustCompile(
			fmt.Sprintf("(?m)%s%s%s$", prefix, dateTimePattern, tag),
		)
	}

	return regexpByTag
}

type Service struct {
	policy  *Policy
	pools   []string
	events  Journal
	matcher SnapshotMatcher
}

func NewService(policy *Policy, events Journal) *Service {
	return &Service{
		policy:  policy,
		pools:   policy.ExtractPools(),
		events:  events,
		matcher: NewSnapshotMatcher(policy.Prefix),
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
			prefix, localTime := s.policy.Prefix, s.policy.LocalTime
			err := CreateSnapshot(t, prefix, tag, localTime, p.Recursive)
			if err != nil {
				log.Printf("cannot snapshot target %q: %s\n", t, err)
			}
			s.Emit(ETCreateSnapshot, t, tag, p.Recursive, err)
		}

		names, err := ListSnapshotNames(t, s.matcher[tag])
		s.Emit(ETListSnapshotNames, t, tag, false, err)
		if err != nil {
			log.Printf("cannot list snapshots of target %q: %s\n", t, err)
			continue
		}
		if len(names) <= int(*keep) {
			continue
		}

		for _, n := range names[int(*keep):] {
			err := DestroySnapshot(t, string(n), p.Recursive)
			if err != nil {
				log.Printf("cannot destroy snapshot \"%s@%s\": %s\n", t, n, err)
			}
			s.Emit(ETDestroySnapshot, fmt.Sprintf("%s@%s", t, n), tag, p.Recursive, err)
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
			s.Emit(ETGetPoolProperty, p, t, false, err)
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
			err := SetPoolProperty(p, key, tickValue)
			if err != nil {
				log.Printf("cannot set property %q of pool %q: %s\n", key, p, err)
			}
			s.Emit(ETSetPoolProperty, p, t, false, err)
		}
	}
}

func (s *Service) Emit(eventType EventType, target, job string, recursive bool, err error) {
	event := Event{
		Type:      eventType,
		Success:   err == nil,
		Timestamp: time.Now(),
		Target:    target,
		Job:       job,
		Recursive: recursive,
		Reason:    ReasonOf(err),
	}

	if err := s.events.Add(event); err != nil {
		log.Printf("cannot add an event (code %d) to the journal: %s\n", eventType, err)
	}
}
