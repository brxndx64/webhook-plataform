package dispatcher

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/brxndx64/webhook-plataform/go/internal/event"
	"github.com/brxndx64/webhook-plataform/go/internal/metrics"
	"github.com/brxndx64/webhook-plataform/go/internal/queue"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

func baseConfig() Config {
	return Config{
		Workers:         2,
		MaxAttempts:     3,
		BackoffBase:     5 * time.Millisecond,
		BackoffMax:      20 * time.Millisecond,
		DeliveryTimeout: 500 * time.Millisecond,
		DLQCapacity:     10,
	}
}

// run enfileira os eventos, roda o pool ate drenar e devolve as metricas.
func run(t *testing.T, cfg Config, evs []event.Event) (*Dispatcher, *metrics.Metrics) {
	t.Helper()
	q := queue.New(len(evs) + 1)
	m := metrics.New("test", cfg.Workers)
	d := New(cfg, q, m, &http.Client{Timeout: 2 * time.Second}, quietLogger())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	d.Start(ctx)
	for _, e := range evs {
		if err := q.Enqueue(e); err != nil {
			t.Fatal(err)
		}
	}
	q.Close()
	d.Wait()
	return d, m
}

func ev(id, dest string) event.Event {
	return event.Event{
		ID: id, Type: "payment.approved", Channel: "webhook",
		Destination: dest, Payload: json.RawMessage(`{"valor":1500}`),
	}
}

func TestEntregaComSucesso(t *testing.T) {
	var recebidos atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recebidos.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	_, m := run(t, baseConfig(), []event.Event{ev("1", srv.URL), ev("2", srv.URL)})

	if m.Delivered.Load() != 2 {
		t.Fatalf("entregues = %d, esperado 2", m.Delivered.Load())
	}
	if recebidos.Load() != 2 {
		t.Fatalf("o destino recebeu %d, esperado 2", recebidos.Load())
	}
	if m.DeadLettered.Load() != 0 {
		t.Errorf("nao deveria haver DLQ, veio %d", m.DeadLettered.Load())
	}
	if m.Delivery.Stats().Count != 2 {
		t.Error("a latencia de entrega deveria ter sido registrada")
	}
}

// O payload precisa chegar ao destino byte a byte, com os cabecalhos
// de rastreio.
func TestRepassaPayloadECabecalhos(t *testing.T) {
	var corpo string
	var idHdr, tipoHdr, ct string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		corpo = string(b)
		idHdr = r.Header.Get("X-Event-Id")
		tipoHdr = r.Header.Get("X-Event-Type")
		ct = r.Header.Get("Content-Type")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	run(t, baseConfig(), []event.Event{ev("evt_abc", srv.URL)})

	if corpo != `{"valor":1500}` {
		t.Errorf("corpo entregue = %q", corpo)
	}
	if idHdr != "evt_abc" || tipoHdr != "payment.approved" {
		t.Errorf("cabecalhos de rastreio errados: id=%q tipo=%q", idHdr, tipoHdr)
	}
	if ct != "application/json" {
		t.Errorf("Content-Type = %q", ct)
	}
}

// 5xx e transitorio: vale tentar de novo.
func TestRetentaEm5xxEDepoisTemSucesso(t *testing.T) {
	var n atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1) < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	_, m := run(t, baseConfig(), []event.Event{ev("1", srv.URL)})

	if m.Delivered.Load() != 1 {
		t.Fatalf("deveria entregar na 3a tentativa, entregues = %d", m.Delivered.Load())
	}
	if m.Retries.Load() != 2 {
		t.Errorf("retentativas = %d, esperado 2", m.Retries.Load())
	}
	if n.Load() != 3 {
		t.Errorf("o destino recebeu %d tentativas, esperado 3", n.Load())
	}
}

// 4xx significa requisicao errada: repetir daria o mesmo erro.
// Gastar tentativas nisso e desperdicio.
func TestNaoRetentaEm4xx(t *testing.T) {
	var n atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	d, m := run(t, baseConfig(), []event.Event{ev("1", srv.URL)})

	if n.Load() != 1 {
		t.Fatalf("deveria tentar UMA vez em 4xx, tentou %d", n.Load())
	}
	if m.Retries.Load() != 0 {
		t.Errorf("nao deveria retentar, retries = %d", m.Retries.Load())
	}
	if m.DeadLettered.Load() != 1 {
		t.Fatalf("deveria ir para a DLQ, veio %d", m.DeadLettered.Load())
	}
	if dlq := d.DLQ(); len(dlq) != 1 || dlq[0].LastStatus != 400 {
		t.Errorf("DLQ inesperada: %+v", dlq)
	}
}

// 429 e "desacelere", nao "desista".
func TestRetentaEm429(t *testing.T) {
	var n atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1) < 2 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	_, m := run(t, baseConfig(), []event.Event{ev("1", srv.URL)})
	if m.Delivered.Load() != 1 {
		t.Fatalf("deveria entregar apos o 429, entregues = %d", m.Delivered.Load())
	}
}

func TestEsgotaTentativasEVaiParaDLQ(t *testing.T) {
	var n atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	cfg := baseConfig()
	cfg.MaxAttempts = 3
	d, m := run(t, cfg, []event.Event{ev("1", srv.URL)})

	if n.Load() != 3 {
		t.Fatalf("esperava 3 tentativas, veio %d", n.Load())
	}
	if m.Delivered.Load() != 0 || m.DeadLettered.Load() != 1 {
		t.Fatalf("entregues=%d dlq=%d", m.Delivered.Load(), m.DeadLettered.Load())
	}
	dlq := d.DLQ()
	if len(dlq) != 1 || dlq[0].Event.ID != "1" || dlq[0].Attempts != 3 {
		t.Errorf("DLQ inesperada: %+v", dlq)
	}
}

// Um destino que aceita a conexao e nunca responde nao pode segurar
// um worker para sempre.
func TestTimeoutPorTentativa(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
	}))
	defer srv.Close()

	cfg := baseConfig()
	cfg.MaxAttempts = 2
	cfg.DeliveryTimeout = 100 * time.Millisecond

	start := time.Now()
	_, m := run(t, cfg, []event.Event{ev("1", srv.URL)})
	elapsed := time.Since(start)

	if m.DeadLettered.Load() != 1 {
		t.Fatalf("deveria ir para a DLQ apos os timeouts, dlq=%d", m.DeadLettered.Load())
	}
	if elapsed > 1500*time.Millisecond {
		t.Errorf("o timeout nao cortou a espera: levou %v", elapsed)
	}
}

func TestDLQRespeitaCapacidade(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	cfg := baseConfig()
	cfg.DLQCapacity = 3
	var evs []event.Event
	for i := 0; i < 10; i++ {
		evs = append(evs, ev(string(rune('a'+i)), srv.URL))
	}
	d, m := run(t, cfg, evs)

	if m.DeadLettered.Load() != 10 {
		t.Errorf("o contador deveria somar 10, veio %d", m.DeadLettered.Load())
	}
	if len(d.DLQ()) != 3 {
		t.Fatalf("a DLQ deveria guardar no maximo 3, guardou %d", len(d.DLQ()))
	}
}

func TestRateLimitPorDestinoEspaca(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := baseConfig()
	cfg.Workers = 4
	cfg.RatePerDestination = 20 // 20/s
	cfg.Burst = 1

	var evs []event.Event
	for i := 0; i < 5; i++ {
		evs = append(evs, ev(string(rune('a'+i)), srv.URL))
	}

	start := time.Now()
	_, m := run(t, cfg, evs)
	elapsed := time.Since(start)

	if m.Delivered.Load() != 5 {
		t.Fatalf("entregues = %d, esperado 5", m.Delivered.Load())
	}
	// 5 entregas a 20/s com rajada 1 levam ~200ms.
	if elapsed < 120*time.Millisecond {
		t.Errorf("o rate limit nao espacou as entregas: %v", elapsed)
	}
}

func TestDestinationKey(t *testing.T) {
	cases := map[string]string{
		"https://a.com/hook/1": "https://a.com",
		"https://a.com/hook/2": "https://a.com",
		"http://a.com:9000/x":  "http://a.com:9000",
		"https://b.com/hook":   "https://b.com",
		"nao-e-url":            "nao-e-url",
	}
	for in, want := range cases {
		if got := destinationKey(in); got != want {
			t.Errorf("destinationKey(%q) = %q, esperado %q", in, got, want)
		}
	}
}
