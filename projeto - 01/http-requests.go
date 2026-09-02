package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

type Evento struct {
	
	ID          string `json:"id"`
	Type        string `json:"type"`
	Channel     string `json:"channel"`
	Destination string `json:"destination"`
	Payload     json.RawMessage `json:"payload"`
}

func healthHandler(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodGet {

		http.Error(w, "metodo nao permitido", http.StatusMethodNotAllowed)

		return

	}
	fmt.Fprintln(w, "ok")
}

func notificationsHandler(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPost {

		http.Error(w, "metodo nao permitido", http.StatusMethodNotAllowed)

		return

	}

	var evento Evento

	if err := json.NewDecoder(r.Body).Decode(&evento); err != nil {

		http.Error(w, "formato inválido", http.StatusBadRequest)
		return
	}

	log.Printf("%+v", evento)

	w.WriteHeader(http.StatusAccepted)

	fmt.Fprintln(w, "202")

	return

}

func main() {

	http.HandleFunc("/health", healthHandler)

	http.HandleFunc("/notifications", notificationsHandler)

	log.Fatal(http.ListenAndServe(":8080", nil))

}
