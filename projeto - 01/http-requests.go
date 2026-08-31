package main

import (
	"fmt"
	"html"
	"log"
	"net/http"
)

func main() {

	http.HandleFunc("/bar", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Ola, %q", html.EscapeString(r.URL.Path))
	})

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {

		if r.Method != http.MethodGet {

			http.Error(w, "metodo nao permitido", http.StatusMethodNotAllowed)

			return

		}
		fmt.Fprintln(w, "ok")
	})

	log.Fatal(http.ListenAndServe(":8080", nil))

}
