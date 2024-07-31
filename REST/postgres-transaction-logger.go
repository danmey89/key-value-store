package main

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	
	_ "github.com/lib/pq"
)

type PostgresTransactionLogger struct {
	events chan<- Event
	errors <-chan error
	db     *sql.DB
}

type Event struct {
	Sequence  uint64
	EventType EventType
	Key       string
	Value     Value
}

type TransactionLogger interface {
	WriteDelete(key string)
	WritePut(key string, value Value)
	ReadEvents() (<-chan Event, <-chan error)
	Run()
	Err() <-chan error
}

type EventType byte

type Value []byte

type dbParams struct {
	dbName   string
	host     string
	user     string
	password string
	sslmode string
}

var config = dbParams{
	dbName:   "kvs",
	host:     "localhost",
	user:     "postgres",
	password: "test123",
	sslmode: "disable",
}

const dbTableName string = "transactions"

const (
	_                     = iota
	EventDelete EventType = iota
	EventPut
)

func (l *PostgresTransactionLogger) WritePut(key string, value Value) {
	l.events <- Event{EventType: EventPut, Key: key, Value: value}
}

func (l *PostgresTransactionLogger) WriteDelete(key string) {
	l.events <- Event{EventType: EventDelete, Key: key}
}

func (l *PostgresTransactionLogger) Err() <-chan error {
	return l.errors
}

func NewPostgresTransactionLogger(config dbParams) (TransactionLogger, error) {
	conn := fmt.Sprintf("host=%s dbname=%s user=%s password=%s sslmode=%s",
		config.host, config.dbName, config.user, config.password, config.sslmode)

	db, err := sql.Open("postgres", conn)
	if err != nil {
		return nil, fmt.Errorf("failed to open db: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to open db connection: %w", err)
	}

	logger := &PostgresTransactionLogger{db: db}

	exists, err := logger.verifyTable()
	if err != nil {
		return nil, fmt.Errorf("failed to verify table exists: %w", err)
	}
	if !exists {
		if err := logger.createTable(); err != nil {
			return nil, fmt.Errorf("failed to create db table: %w", err)
		}
	}

	return logger, nil
}

func (l *PostgresTransactionLogger) verifyTable() (bool, error) {

	var result string

	rows, err := l.db.Query(fmt.Sprintf("SELECT to_regclass('public.%s');", dbTableName))
	if err != nil {
		return false, err
	}
	defer rows.Close()

	for rows.Next() && result != dbTableName {
		rows.Scan(&result)
	}

	return result == dbTableName, rows.Err()
}

func (l *PostgresTransactionLogger) createTable() error {

	query := fmt.Sprintf(`CREATE TABLE %s(
 						ID SERIAL PRIMARY KEY,
 						event_type SMALLINT,
 						key VARCHAR,
 						value TEXT);`,
		dbTableName)

	if _, err := l.db.Exec(query); err != nil {
		return err
	}
	return nil
}

func (l *PostgresTransactionLogger) ReadEvents() (<-chan Event, <-chan error) {

	outEvent := make(chan Event)
	outError := make(chan error, 1)

	go func() {
		var e Event

		defer close(outEvent)
		defer close(outError)

		rows, err := l.db.Query(fmt.Sprintf("SELECT * FROM %s", dbTableName))
		if err != nil {
			outError <- fmt.Errorf("sql query error: %w", err)
			return
		}

		defer rows.Close()

		for rows.Next() {
			if err := rows.Scan(&e.Sequence, &e.EventType, &e.Key, &e.Value); err != nil {
				outError <- err
				return
			}

			outEvent <- e
		}

		if err = rows.Err(); err != nil {
			outError <- fmt.Errorf("transaction log read error: %w", err)
		}

	}()
	return outEvent, outError
}

func (l *PostgresTransactionLogger) Run() {

	events := make(chan Event, 16)
	l.events = events

	errors := make(chan error, 1)
	l.errors = errors

	go func() {

		query := fmt.Sprintf("INSERT INTO %s event_type, key, value VALUES (?, ?, ?);", dbTableName)

		statement, err := l.db.Prepare(query)
		if err != nil {
			errors <- err
			return
		}

		for e := range events {

			if _, err = statement.Exec(e.EventType, e.Key, e.Value); err != nil {
				errors <- err
				return
			}
		}
	}()
}

func (src Value) encode() []byte {

	dst := make([]byte, hex.EncodedLen(len(src)))
	hex.Encode(dst, src)

	return dst
}

func (src Value) decode() []byte {
	dst := make([]byte, hex.DecodedLen(len(src)))
	_, err := hex.Decode(dst, src)
	if err != nil {
		fmt.Errorf("decoding error: %w", err)
	}

	return dst
}
