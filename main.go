package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func main() {
	policyPath := flag.String("p", "", "path to the policy file")
	journalPath := flag.String("j", "", "path to the journal file (optional)")
	flag.Parse()

	if *policyPath == "" {
		log.Fatalln("path to the policy file is not specified")
	}

	policy, err := LoadPolicy(*policyPath)
	if err != nil {
		log.Fatalf("cannot load the policy: %s\n", err)
	}

	journal := Journal(NilJournal{})
	if *journalPath != "" {
		sj, err := NewSQLJournal(*journalPath)
		if err != nil {
			log.Fatalf("cannot open the journal: %s\n", err)
		}
		journal = sj
	}

	service := NewService(policy, journal)

	var wg sync.WaitGroup
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-shutdown:
			log.Println("(!) waiting for jobs to finish")
			wg.Wait()
			log.Println("(!) exiting")
			return
		case t := <-ticker.C:
			switch t.Minute() {
			case 0:
				wg.Add(1)
				go func() {
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
