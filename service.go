package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/mux"
)

var store = struct {
	sync.RWMutex
	m map[string]Value
}{m: make(map[string]Value)}

var ErrorNoSuchKey = errors.New("no such key")

var logger TransactionLogger

func main() {

	err := initializeTransactionLog()
	if err != nil {
		panic(err)
	}

	r := mux.NewRouter()

	r.HandleFunc("/v1/{key}", PutHandler).Methods("PUT")
	r.HandleFunc("/v1/{key}", GetHandler).Methods("GET")
	r.HandleFunc("/v1/{key}", DeleteHandler).Methods("DELETE")

	r.HandleFunc("/v1/{key}", NotAllowedHandler)
	r.HandleFunc("/v1/", NotAllowedHandler)

	log.Fatal(http.ListenAndServe(":8080", r))

}

func initializeTransactionLog() error {
	var err error

	logger, err = NewFileTransactionLogger("transaction.log")
	if err != nil {
		return fmt.Errorf("unable to create event logger: %w", err)
	}

	events, errors := logger.ReadEvents()
	e, ok := Event{}, true

	for ok && err == nil {
		select {
		case err, ok = <-errors:
		case e, ok = <-events:
			switch e.EventType {
			case EventDelete:
				err = Delete(e.Key)
			case EventPut:
				err = Put(e.Key, e.Value)
			}
		}
	}

	logger.Run()

	return err
}

func NotAllowedHandler(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "Not Allowed", http.StatusMethodNotAllowed)
}

func PutHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := io.ReadAll(r.Body)
	defer r.Body.Close()

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}

	if err := Put(key, value); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}

	w.WriteHeader(http.StatusCreated)
	logger.WritePut(key, value)

}

func GetHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	res, err := Get(key)

	if err != nil {

		if errors.Is(err, ErrorNoSuchKey) {
			http.Error(w, err.Error(), http.StatusNotFound)

			return

		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)

			return
		}
	}

	w.WriteHeader(http.StatusOK)
	w.Write(res)
}

func DeleteHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	if err := Delete(key); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}

	w.WriteHeader(http.StatusOK)
	logger.WriteDelete(key)

}

func Put(key string, value Value) error {
	store.Lock()
	store.m[key] = value
	store.Unlock()

	return nil

}

func Get(key string) (Value, error) {
	store.RLock()
	value, ok := store.m[key]
	store.RUnlock()

	if !ok {
		return nil, ErrorNoSuchKey
	}

	return value.decode(), nil

}

func Delete(key string) error {
	store.Lock()
	delete(store.m, key)
	store.Unlock()

	return nil
}
