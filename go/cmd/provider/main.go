// Comando provider: simulador do sistema externo que recebe os webhooks.
//
// Serve para tornar o benchmark reproduzivel. Um destino real varia de
// latencia a cada minuto; aqui a latencia, o jitter e a taxa de erro
// sao parametros fixos, entao Go e Python enfrentam exatamente as
// mesmas condicoes.
//
//	go run ./cmd/provider -addr :9000 -latency 20ms -error-rate 0.05
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"sync/atomic"
	"time"
)

type counters struct {
	received atomic.Int64
	ok       atomic.Int64
	failed   atomic.Int64
	hung     atomic.Int64
}

func main() {
	var (
		addr      = flag.String("addr", ":9000", "endereco de escuta")
		latency   = flag.Duration("latency", 20*time.Millisecond, "latencia base da resposta")
		jitter    = flag.Duration("jitter", 10*time.Millisecond, "variacao aleatoria somada a latencia")
		errorRate = flag.Float64("error-rate", 0, "fracao de respostas 5xx (0 a 1)")
		hangRate  = flag.Float64("hang-rate", 0, "fracao de requisicoes que travam ate o timeout do cliente")
		hangFor   = flag.Duration("hang-for", 30*time.Second, "quanto tempo travar")
	)
	flag.Parse()

	var c counters
	start := time.Now()

	mux := http.NewServeMux()

	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"received":       c.received.Load(),
			"ok":             c.ok.Load(),
			"failed":         c.failed.Load(),
			"hung":           c.hung.Load(),
			"uptime_seconds": time.Since(start).Seconds(),
		})
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		c.received.Add(1)

		// Ler e descartar o corpo mantem a conexao reaproveitavel.
		_, _ = io.Copy(io.Discard, r.Body)
		_ = r.Body.Close()

		if *hangRate > 0 && rand.Float64() < *hangRate {
			c.hung.Add(1)
			time.Sleep(*hangFor)
			return
		}

		d := *latency
		if *jitter > 0 {
			d += time.Duration(rand.Int64N(int64(*jitter)))
		}
		if d > 0 {
			time.Sleep(d)
		}

		if *errorRate > 0 && rand.Float64() < *errorRate {
			c.failed.Add(1)
			http.Error(w, "indisponivel", http.StatusServiceUnavailable)
			return
		}

		c.ok.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprint(w, `{"ok":true}`)
	})

	srv := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Printf("provedor simulado em %s (latencia=%v jitter=%v erro=%.0f%% hang=%.0f%%)",
		*addr, *latency, *jitter, *errorRate*100, *hangRate*100)
	log.Fatal(srv.ListenAndServe())
}
