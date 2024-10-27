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

type server struct {
	db    localdb.Manager
	table *localdb.CurrentTable
}

// Serve is a HTTP Server layer
func Serve(db localdb.Manager, table *localdb.CurrentTable) {
	server := &server{db: db, table: table}
	http.HandleFunc("/use", server.handlerUse)
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

func (s *server) handlerUse(w http.ResponseWriter, r *http.Request) {
	indexName := r.URL.Query().Get("indexName")
	err := s.db.Use(s.table, indexName)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(&AppError{Error: err.Error(), Code: http.StatusInternalServerError})
	}
}

func (s *server) handlerFirst(w http.ResponseWriter, _ *http.Request) {
	dat, eof, err := s.db.First(s.table)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(&AppError{Error: err.Error(), Code: http.StatusInternalServerError})
		return
	}

	if eof {
		w.WriteHeader(http.StatusResetContent)
		json.NewEncoder(w).Encode(&AppError{Error: "beginning of table", Code: http.StatusResetContent})
		return
	}

	json.NewEncoder(w).Encode(dat)
}

func (s *server) handlerLast(w http.ResponseWriter, _ *http.Request) {
	dat, eof, err := s.db.Last(s.table)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(&AppError{Error: err.Error(), Code: http.StatusInternalServerError})
		return
	}

	if eof {
		w.WriteHeader(http.StatusResetContent)
		json.NewEncoder(w).Encode(&AppError{Error: "beginning of table", Code: http.StatusResetContent})
		return
	}

	json.NewEncoder(w).Encode(dat)
}

func (s *server) handlerNext(w http.ResponseWriter, _ *http.Request) {
	dat, eof, err := s.db.Next(s.table)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(&AppError{Error: err.Error(), Code: http.StatusInternalServerError})
		return
	}

	if eof {
		w.WriteHeader(http.StatusResetContent)
		json.NewEncoder(w).Encode(&AppError{Error: "end of table", Code: http.StatusResetContent})
		return
	}

	json.NewEncoder(w).Encode(dat)
}

func (s *server) handlerPrev(w http.ResponseWriter, _ *http.Request) {
	dat, bof, err := s.db.Prev(s.table)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(&AppError{Error: err.Error(), Code: http.StatusInternalServerError})
		return
	}

	if bof {
		w.WriteHeader(http.StatusResetContent)
		json.NewEncoder(w).Encode(&AppError{Error: "beginning of table", Code: http.StatusResetContent})
		return
	}

	json.NewEncoder(w).Encode(dat)
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
