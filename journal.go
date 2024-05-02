package main

import (
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

type Severity int

const (
	Success Severity = iota
	Warning
	Error
)

type EventType int

const (
	ECreateSnapshot EventType = iota
	EListSnapshotNames
	EDestroySnapshot
	EGetPoolProperty
	EIgnoreTimestamp
	ESetPoolProperty
	EStopService
)

type Event struct {
	Type        EventType
	Severity    Severity
	Timestamp   time.Time
	Target      string
	Job         string
	Recursive   bool
	Description string
}

type SQLJournal struct {
	db *sql.DB
}

func NewSQLJournal(path string) (*SQLJournal, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS events(
		type INTEGER NOT NULL,
		severity INTEGER NOT NULL,
		timestamp INTEGER NOT NULL,
		target TEXT NOT NULL,
		job TEXT NOT NULL,
		recursive INTEGER NOT NULL,
		description TEXT NOT NULL
	)`)
	if err != nil {
		return nil, fmt.Errorf("cannot create events table: %w", err)
	}

	return &SQLJournal{db}, nil
}

func (sj *SQLJournal) Append(event Event) error {
	recursive := 0
	if event.Recursive {
		recursive = 1
	}

	_, err := sj.db.Exec(`INSERT INTO events VALUES(?, ?, ?, ?, ?, ?, ?)`,
		int(event.Type),
		int(event.Severity),
		event.Timestamp.Unix(),
		event.Target,
		event.Job,
		recursive,
		event.Description,
	)

	return err
}

type NilJournal struct{}

func (nj NilJournal) Append(event Event) error {
	return nil
}
