package main

import (
	"fmt"
	"log"
	"net/http"
)

func WandR(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodGet {

		http.Error(w, "metodo nao permitido", http.StatusMethodNotAllowed)

		return

	}
	fmt.Fprintln(w, "ok")
}

func main() {

	http.HandleFunc("/health", WandR)

	log.Fatal(http.ListenAndServe(":8080", nil))

}
