// Package api expoe o contrato HTTP.
//
// O mesmo contrato e implementado em Python (../python/app.py). Rotas,
// codigos de status, formato de erro e formato de /stats sao identicos,
// porque sem isso a comparacao de performance nao significaria nada.
package api

import (
	_ "embed"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/brxndx64/webhook-plataform/go/internal/dedup"
	"github.com/brxndx64/webhook-plataform/go/internal/dispatcher"
	"github.com/brxndx64/webhook-plataform/go/internal/event"
	"github.com/brxndx64/webhook-plataform/go/internal/metrics"
	"github.com/brxndx64/webhook-plataform/go/internal/queue"
)

//go:embed dashboard.html
var dashboardHTML []byte

// Deps sao as dependencias do servidor. Injetadas, e nao globais, para
// que os testes montem um servidor completo em memoria sem abrir porta.
type Deps struct {
	Validator    *event.Validator
	Queue        *queue.Queue
	Dedup        *dedup.Store
	Metrics      *metrics.Metrics
	Dispatcher   *dispatcher.Dispatcher
	Logger       *slog.Logger
	MaxBodyBytes int64
}

type acceptResponse struct {
	Status     string `json:"status"`
	ID         string `json:"id"`
	Duplicate  bool   `json:"duplicate"`
	QueueDepth int    `json:"queue_depth"`
}

type errorResponse struct {
	Error    string   `json:"error"`
	Problems []string `json:"problems,omitempty"`
}

// NewHandler monta o roteador.
func NewHandler(d Deps) http.Handler {
	if d.MaxBodyBytes <= 0 {
		d.MaxBodyBytes = 1 << 20 // 1 MiB
	}
	mux := http.NewServeMux()

	mux.HandleFunc("/health", d.health)
	mux.HandleFunc("/notifications", d.notifications)
	mux.HandleFunc("/stats", d.stats)
	mux.HandleFunc("/metrics", d.prometheus)
	mux.HandleFunc("/dlq", d.dlqHandler)
	mux.HandleFunc("/admin/reset", d.reset)
	mux.HandleFunc("/dashboard", d.dashboard)
	mux.HandleFunc("/", d.root)

	return mux
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	// Ordem obrigatoria: header, depois status, depois corpo. Fora
	// dela o Go ignora em silencio e envia 200.
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (d Deps) health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		d.Metrics.MethodNotAllowed.Add(1)
		writeJSON(w, http.StatusMethodNotAllowed, errorResponse{Error: "metodo nao permitido"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// notifications e a porta de entrada da plataforma.
//
// A ordem das etapas nao e negociavel: tudo que pode falhar acontece
// ANTES de qualquer byte ser escrito na resposta. Depois do primeiro
// Write o status ja foi enviado e nao ha como voltar atras.
func (d Deps) notifications(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	defer func() {
		d.Metrics.API.ObserveMicros(float64(time.Since(start).Microseconds()))
	}()

	if r.Method != http.MethodPost {
		d.Metrics.MethodNotAllowed.Add(1)
		writeJSON(w, http.StatusMethodNotAllowed, errorResponse{Error: "metodo nao permitido"})
		return
	}
	d.Metrics.Received.Add(1)

	// 1. Limite de corpo: sem isso um cliente pode mandar um corpo
	// infinito e derrubar o processo por memoria.
	r.Body = http.MaxBytesReader(w, r.Body, d.MaxBodyBytes)

	// 2. Decodificar
	var e event.Event
	if err := json.NewDecoder(r.Body).Decode(&e); err != nil {
		d.Metrics.Invalid.Add(1)
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "JSON invalido: " + err.Error()})
		return
	}

	// 3. Validar
	if err := d.Validator.Validate(e); err != nil {
		d.Metrics.Invalid.Add(1)
		var ve *event.ValidationError
		if errors.As(err, &ve) {
			writeJSON(w, http.StatusBadRequest, errorResponse{
				Error: "evento invalido", Problems: ve.Problems})
			return
		}
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
		return
	}
	e.Normalize()

	// 4. Idempotencia. Note que a resposta e 202, nao erro: para o
	// cliente que reenviou porque perdeu a resposta, o resultado
	// final e o mesmo - o evento esta aceito e sera processado
	// exatamente uma vez. Devolver erro aqui faria um retry legitimo
	// parecer falha.
	if d.Dedup.CheckAndMark(e.ID) {
		d.Metrics.Duplicated.Add(1)
		writeJSON(w, http.StatusAccepted, acceptResponse{
			Status: "duplicate", ID: e.ID, Duplicate: true, QueueDepth: d.Queue.Len()})
		return
	}

	// 5. Enfileirar. Fila cheia vira 503 com Retry-After: backpressure,
	// ou seja, recusar explicitamente em vez de aceitar trabalho que
	// nao se consegue fazer e degradar para todo mundo.
	if err := d.Queue.Enqueue(e); err != nil {
		// Desfazer a marcacao de idempotencia: o evento NAO foi aceito,
		// entao a retentativa que o Retry-After pede precisa passar.
		// Sem isto o evento se perderia em silencio.
		d.Dedup.Unmark(e.ID)
		d.Metrics.RejectedFull.Add(1)
		w.Header().Set("Retry-After", "1")
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "fila cheia, tente novamente"})
		return
	}

	// 6. So agora responder.
	d.Metrics.Accepted.Add(1)
	writeJSON(w, http.StatusAccepted, acceptResponse{
		Status: "accepted", ID: e.ID, QueueDepth: d.Queue.Len()})
}

func (d Deps) stats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, errorResponse{Error: "metodo nao permitido"})
		return
	}
	writeJSON(w, http.StatusOK, d.Metrics.Snapshot())
}

func (d Deps) prometheus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	d.Metrics.WritePrometheus(w)
}

func (d Deps) dlqHandler(w http.ResponseWriter, r *http.Request) {
	if d.Dispatcher == nil {
		writeJSON(w, http.StatusOK, []dispatcher.DeadLetter{})
		return
	}
	writeJSON(w, http.StatusOK, d.Dispatcher.DLQ())
}

func (d Deps) reset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, errorResponse{Error: "metodo nao permitido"})
		return
	}
	d.Metrics.Reset()
	// Limpar tambem a memoria de idempotencia: os IDs gerados durante
	// o aquecimento colidiriam com os da fase medida e apareceriam
	// como duplicatas.
	d.Dedup.Clear()
	writeJSON(w, http.StatusOK, map[string]string{"status": "reset"})
}

func (d Deps) dashboard(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(dashboardHTML)
}

func (d Deps) root(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		writeJSON(w, http.StatusNotFound, errorResponse{Error: "rota nao encontrada"})
		return
	}
	http.Redirect(w, r, "/dashboard", http.StatusFound)
}
