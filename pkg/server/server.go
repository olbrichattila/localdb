// Package server exposes the database engine as Json on http
package server

import (
	"encoding/json"
	"fmt"
	localdb "godb/pkg/db"
	"net/http"
	"strconv"
)

// AppError is when an error is returned
type AppError struct {
	Error string `json:"error"`
	Code  int    `json:"code"`
}

// Stat is a structure containing the JSON representation of database statistics, currently RecordCount only.
type Stat struct {
	RecordCount int64 `json:"recordCount"`
}

type RecordStatus struct {
	Eof bool `json:"eof"`
	Bof bool `json:"bof"`
}

type server struct {
	db    localdb.Manager
	table *localdb.CurrentTable
}

// Serve is a HTTP Server layer
func Serve(db localdb.Manager, table *localdb.CurrentTable) {
	server := &server{db: db, table: table}
	http.HandleFunc("/struct", server.handlerStruct)
	http.HandleFunc("/recCount", server.handlerRecCount)
	http.HandleFunc("/use", server.handlerUse)
	http.HandleFunc("/fetch", server.handlerFetch)
	http.HandleFunc("/fetchCurrent", server.handlerFetchCurrent)
	http.HandleFunc("/first", server.handlerFirst)
	http.HandleFunc("/last", server.handlerLast)
	http.HandleFunc("/next", server.handlerNext)
	http.HandleFunc("/prev", server.handlerPrev)
	http.HandleFunc("/insert", server.handlerInsert)
	http.HandleFunc("/seek", server.handlerSeek)
	http.HandleFunc("/delete", server.handlerDelete)

	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		fmt.Println(err)
	}
}

func (s *server) handlerStruct(w http.ResponseWriter, _ *http.Request) {
	structure := s.table.Struct()

	json.NewEncoder(w).Encode(structure)
}

func (s *server) handlerRecCount(w http.ResponseWriter, _ *http.Request) {
	rc, err := s.db.RecCount(s.table)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(&AppError{Error: err.Error(), Code: http.StatusInternalServerError})
		return
	}

	stat := &Stat{RecordCount: rc}
	json.NewEncoder(w).Encode(stat)
}

func (s *server) handlerUse(w http.ResponseWriter, r *http.Request) {
	indexName := r.URL.Query().Get("indexName")
	err := s.db.Use(s.table, indexName)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(&AppError{Error: err.Error(), Code: http.StatusInternalServerError})
	}
}

func (s *server) handlerFetch(w http.ResponseWriter, r *http.Request) {
	val := r.URL.Query().Get("id")
	num, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(&AppError{Error: "record ID is not a number", Code: http.StatusInternalServerError})
		return
	}

	dat, eof, _, err := s.db.Fetch(s.table, num)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(&AppError{Error: err.Error(), Code: http.StatusInternalServerError})
		return
	}

	if eof {
		json.NewEncoder(w).Encode(&RecordStatus{Eof: true})
		return
	}

	json.NewEncoder(w).Encode(dat)
}

func (s *server) handlerFetchCurrent(w http.ResponseWriter, _ *http.Request) {
	dat, eof, _, err := s.db.FetchCurrent(s.table)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(&AppError{Error: err.Error(), Code: http.StatusInternalServerError})
		return
	}

	if eof {
		json.NewEncoder(w).Encode(&RecordStatus{Eof: true})
		return
	}

	json.NewEncoder(w).Encode(dat)
}

func (s *server) handlerFirst(w http.ResponseWriter, _ *http.Request) {
	err := s.db.First(s.table)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(&AppError{Error: err.Error(), Code: http.StatusInternalServerError})
		return
	}
}

func (s *server) handlerLast(w http.ResponseWriter, _ *http.Request) {
	err := s.db.Last(s.table)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(&AppError{Error: err.Error(), Code: http.StatusInternalServerError})
		return
	}
}

func (s *server) handlerNext(w http.ResponseWriter, _ *http.Request) {
	eof, err := s.db.Next(s.table)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(&AppError{Error: err.Error(), Code: http.StatusInternalServerError})
		return
	}

	if eof {
		json.NewEncoder(w).Encode(&RecordStatus{Eof: true})
		return
	}

	json.NewEncoder(w).Encode(&RecordStatus{})
}

func (s *server) handlerPrev(w http.ResponseWriter, _ *http.Request) {
	bof, err := s.db.Prev(s.table)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(&AppError{Error: err.Error(), Code: http.StatusInternalServerError})
		return
	}

	if bof {
		json.NewEncoder(w).Encode(&RecordStatus{Bof: true})
		return
	}

	json.NewEncoder(w).Encode(&RecordStatus{})
}

func (s *server) handlerInsert(w http.ResponseWriter, r *http.Request) {
	var data map[string]interface{}
	err := json.NewDecoder(r.Body).Decode(&data)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(&AppError{Error: err.Error(), Code: http.StatusInternalServerError})
		return
	}
	defer r.Body.Close()

	_, err = s.db.Insert(s.table, data)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(&AppError{Error: err.Error(), Code: http.StatusInternalServerError})
		return
	}
}

func (s *server) handlerSeek(w http.ResponseWriter, r *http.Request) {
	val := r.URL.Query().Get("value")
	err := s.db.Seek(s.table, val)

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(&AppError{Error: err.Error(), Code: http.StatusInternalServerError})
		return
	}
}

func (s *server) handlerDelete(w http.ResponseWriter, r *http.Request) {
	val := r.URL.Query().Get("recNo")
	rn, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(&AppError{Error: err.Error(), Code: http.StatusInternalServerError})
	}

	err = s.db.Delete(s.table, rn)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(&AppError{Error: err.Error(), Code: http.StatusInternalServerError})
	}
}
