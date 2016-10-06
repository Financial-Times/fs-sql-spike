package main

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"net/http"
)

type handlers struct {
	theDB *orgDB
}

func (h handlers) idHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	o, found, err := h.theDB.getOrg(vars["uuid"])
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !found {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	j, err := json.Marshal(o)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	w.Write(j)
	w.Write([]byte("\n"))
}

func (h handlers) countHandler(w http.ResponseWriter, r *http.Request) {
	size, err := h.theDB.size()
	if err != nil {
		http.Error(w, "failed to get db size", http.StatusInternalServerError)
		return
	}
	if err := json.NewEncoder(w).Encode(size); err != nil {
		http.Error(w, "failed to serialise count", http.StatusInternalServerError)
		return
	}
}

func (h handlers) listHandler(w http.ResponseWriter, r *http.Request) {
	enc := json.NewEncoder(w)
	h.theDB.forEachId(func(uuid string) error {
		type id struct {
			ID string `json:"id"`
		}
		err := enc.Encode(id{uuid})
		if err != nil {
			return err
		}
		return nil
	})
}
