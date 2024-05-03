package main

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"
)

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
		success INTEGER NOT NULL,
		timestamp INTEGER NOT NULL,
		target TEXT NOT NULL,
		job TEXT NOT NULL,
		recursive INTEGER NOT NULL,
		reason TEXT NOT NULL
	)`)
	if err != nil {
		return nil, fmt.Errorf("cannot create events table: %w", err)
	}

	return &SQLJournal{db}, nil
}

func (sj *SQLJournal) Add(event Event) error {
	success := 0
	if event.Success {
		success = 1
	}

	recursive := 0
	if event.Recursive {
		recursive = 1
	}

	_, err := sj.db.Exec(`INSERT INTO events VALUES(?, ?, ?, ?, ?, ?, ?)`,
		int(event.Type),
		success,
		event.Timestamp.Unix(),
		event.Target,
		event.Job,
		recursive,
		event.Reason,
	)

	return err
}

type NilJournal struct{}

func (nj NilJournal) Add(event Event) error {
	return nil
}
