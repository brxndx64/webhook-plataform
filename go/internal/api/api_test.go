package api

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/brxndx64/webhook-plataform/go/internal/dedup"
	"github.com/brxndx64/webhook-plataform/go/internal/event"
	"github.com/brxndx64/webhook-plataform/go/internal/metrics"
	"github.com/brxndx64/webhook-plataform/go/internal/queue"
)

// Estes testes chamam o handler direto, em memoria: sem abrir porta,
// sem rede, sem servidor. Por isso rodam em milissegundos - e por isso
// os handlers precisaram virar funcoes nomeadas com dependencias
// injetadas em vez de funcoes anonimas dentro do main.
func newTestAPI(t *testing.T, queueCap int) (http.Handler, *queue.Queue, *metrics.Metrics) {
	t.Helper()
	q := queue.New(queueCap)
	m := metrics.New("go-test", 1)
	m.QueueLen, m.QueueCap = q.Len, q.Cap
	d := dedup.New(time.Minute)
	m.DedupSize = d.Len

	h := NewHandler(Deps{
		Validator:    event.NewValidator(nil, nil, true, 4096),
		Queue:        q,
		Dedup:        d,
		Metrics:      m,
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxBodyBytes: 4096,
	})
	return h, q, m
}

func do(h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	var r *http.Request
	if body == "" {
		r = httptest.NewRequest(method, path, nil)
	} else {
		r = httptest.NewRequest(method, path, strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

const eventoValido = `{"id":"evt_1","type":"payment.approved","channel":"webhook",` +
	`"destination":"http://localhost:9000/hook","payload":{"valor":1500}}`

func TestHealth(t *testing.T) {
	h, _, _ := newTestAPI(t, 10)

	w := do(h, http.MethodGet, "/health", "")
	if w.Code != http.StatusOK {
		t.Fatalf("GET /health = %d, esperado 200", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("Content-Type = %q, esperado application/json", ct)
	}

	if w := do(h, http.MethodPost, "/health", ""); w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST /health = %d, esperado 405", w.Code)
	}
}

func TestNotificationsAceitaEEnfileira(t *testing.T) {
	h, q, m := newTestAPI(t, 10)

	w := do(h, http.MethodPost, "/notifications", eventoValido)
	if w.Code != http.StatusAccepted {
		t.Fatalf("= %d (%s), esperado 202", w.Code, w.Body.String())
	}

	var resp acceptResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("resposta nao e JSON: %v", err)
	}
	if resp.Status != "accepted" || resp.ID != "evt_1" {
		t.Errorf("resposta inesperada: %+v", resp)
	}
	if q.Len() != 1 {
		t.Errorf("esperava 1 item na fila, veio %d", q.Len())
	}
	if m.Accepted.Load() != 1 {
		t.Errorf("contador de aceitos = %d, esperado 1", m.Accepted.Load())
	}

	e := <-q.C()
	if string(e.Payload) != `{"valor":1500}` {
		t.Errorf("payload alterado: %s", e.Payload)
	}
}

func TestNotificationsMetodoErrado(t *testing.T) {
	h, _, m := newTestAPI(t, 10)
	for _, mth := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
		if w := do(h, mth, "/notifications", ""); w.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s = %d, esperado 405", mth, w.Code)
		}
	}
	if m.MethodNotAllowed.Load() != 3 {
		t.Errorf("contador 405 = %d, esperado 3", m.MethodNotAllowed.Load())
	}
}

func TestNotificationsJSONMalformado(t *testing.T) {
	h, q, m := newTestAPI(t, 10)

	w := do(h, http.MethodPost, "/notifications", `{"id":"1","type":`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("= %d, esperado 400", w.Code)
	}
	if q.Len() != 0 {
		t.Error("JSON invalido nao pode chegar a fila")
	}
	if m.Invalid.Load() != 1 {
		t.Errorf("contador de invalidos = %d, esperado 1", m.Invalid.Load())
	}
}

func TestNotificationsCamposObrigatorios(t *testing.T) {
	h, _, _ := newTestAPI(t, 10)

	w := do(h, http.MethodPost, "/notifications", `{}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("= %d, esperado 400", w.Code)
	}
	var resp errorResponse
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if len(resp.Problems) < 3 {
		t.Fatalf("esperava os problemas listados de uma vez, veio %v", resp.Problems)
	}
}

// Idempotencia: o reenvio recebe 202, nao erro. Para o cliente que
// perdeu a resposta, o resultado final e o mesmo - aceito uma vez.
func TestNotificationsDeduplicaMasResponde202(t *testing.T) {
	h, q, m := newTestAPI(t, 10)

	w1 := do(h, http.MethodPost, "/notifications", eventoValido)
	w2 := do(h, http.MethodPost, "/notifications", eventoValido)

	if w1.Code != http.StatusAccepted || w2.Code != http.StatusAccepted {
		t.Fatalf("ambas deveriam ser 202, vieram %d e %d", w1.Code, w2.Code)
	}
	var r2 acceptResponse
	_ = json.Unmarshal(w2.Body.Bytes(), &r2)
	if !r2.Duplicate {
		t.Error("a segunda resposta deveria marcar duplicate=true")
	}
	if q.Len() != 1 {
		t.Fatalf("o evento deveria ter sido enfileirado UMA vez, veio %d", q.Len())
	}
	if m.Duplicated.Load() != 1 {
		t.Errorf("contador de duplicados = %d, esperado 1", m.Duplicated.Load())
	}
}

// Backpressure: com a fila cheia, recusar explicitamente e melhor que
// aceitar trabalho que nao se consegue fazer.
func TestNotificationsFilaCheiaDevolve503(t *testing.T) {
	h, _, m := newTestAPI(t, 1)

	do(h, http.MethodPost, "/notifications", eventoValido)

	segundo := strings.Replace(eventoValido, "evt_1", "evt_2", 1)
	w := do(h, http.MethodPost, "/notifications", segundo)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("= %d, esperado 503", w.Code)
	}
	if w.Header().Get("Retry-After") == "" {
		t.Error("503 deveria trazer Retry-After")
	}
	if m.RejectedFull.Load() != 1 {
		t.Errorf("contador de fila cheia = %d, esperado 1", m.RejectedFull.Load())
	}
}

func TestNotificationsCorpoGrandeDemais(t *testing.T) {
	h, _, _ := newTestAPI(t, 10)
	grande := `{"id":"1","type":"payment.approved","destination":"http://localhost:9000/h",` +
		`"payload":{"x":"` + strings.Repeat("a", 8000) + `"}}`
	if w := do(h, http.MethodPost, "/notifications", grande); w.Code != http.StatusBadRequest {
		t.Fatalf("= %d, esperado 400 para corpo acima do limite", w.Code)
	}
}

func TestStatsDevolveSnapshot(t *testing.T) {
	h, _, _ := newTestAPI(t, 10)
	do(h, http.MethodPost, "/notifications", eventoValido)

	w := do(h, http.MethodGet, "/stats", "")
	if w.Code != http.StatusOK {
		t.Fatalf("= %d, esperado 200", w.Code)
	}
	var s metrics.Snapshot
	if err := json.Unmarshal(w.Body.Bytes(), &s); err != nil {
		t.Fatalf("stats nao e JSON valido: %v", err)
	}
	if s.Accepted != 1 || s.Impl != "go-test" {
		t.Errorf("snapshot inesperado: aceitos=%d impl=%q", s.Accepted, s.Impl)
	}
	if s.QueueCap != 10 {
		t.Errorf("queue_cap = %d, esperado 10", s.QueueCap)
	}
}

func TestResetZeraContadores(t *testing.T) {
	h, _, m := newTestAPI(t, 10)
	do(h, http.MethodPost, "/notifications", eventoValido)

	if w := do(h, http.MethodPost, "/admin/reset", ""); w.Code != http.StatusOK {
		t.Fatalf("reset = %d", w.Code)
	}
	if m.Accepted.Load() != 0 {
		t.Errorf("apos reset, aceitos = %d", m.Accepted.Load())
	}
}

func TestMetricsPrometheus(t *testing.T) {
	h, _, _ := newTestAPI(t, 10)
	w := do(h, http.MethodGet, "/metrics", "")
	if w.Code != http.StatusOK {
		t.Fatalf("= %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "webhook_accepted_total") {
		t.Error("faltou a metrica webhook_accepted_total")
	}
}

func TestRotaInexistente(t *testing.T) {
	h, _, _ := newTestAPI(t, 10)
	if w := do(h, http.MethodGet, "/nao-existe", ""); w.Code != http.StatusNotFound {
		t.Fatalf("= %d, esperado 404", w.Code)
	}
}

func BenchmarkNotifications(b *testing.B) {
	q := queue.New(1 << 20)
	m := metrics.New("bench", 1)
	m.QueueLen, m.QueueCap = q.Len, q.Cap
	h := NewHandler(Deps{
		Validator:    event.NewValidator(nil, nil, true, 4096),
		Queue:        q,
		Dedup:        dedup.New(time.Minute),
		Metrics:      m,
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxBodyBytes: 4096,
	})
	body := []byte(eventoValido)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := httptest.NewRequest(http.MethodPost, "/notifications", bytes.NewReader(body))
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
	}
}
