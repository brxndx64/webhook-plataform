// Package dispatcher consome a fila e entrega os webhooks.
//
// E aqui que vive a parte dificil: concorrencia controlada, timeout,
// retry com backoff, rate limit por destino e dead-letter queue.
package dispatcher

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand/v2"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/brxndx64/webhook-plataform/go/internal/event"
	"github.com/brxndx64/webhook-plataform/go/internal/metrics"
	"github.com/brxndx64/webhook-plataform/go/internal/queue"
	"github.com/brxndx64/webhook-plataform/go/internal/ratelimit"
)

// Config parametriza o pool.
type Config struct {
	Workers            int
	MaxAttempts        int
	BackoffBase        time.Duration
	BackoffMax         time.Duration
	DeliveryTimeout    time.Duration
	RatePerDestination float64
	Burst              int
	DLQCapacity        int
}

// DeadLetter e um evento que esgotou as tentativas.
type DeadLetter struct {
	Event      event.Event `json:"event"`
	Attempts   int         `json:"attempts"`
	LastStatus int         `json:"last_status"`
	LastError  string      `json:"last_error"`
	At         time.Time   `json:"at"`
}

// Dispatcher e o pool de workers.
type Dispatcher struct {
	cfg    Config
	q      *queue.Queue
	m      *metrics.Metrics
	client *http.Client
	log    *slog.Logger
	lim    *ratelimit.Limiter

	dlqMu sync.Mutex
	dlq   []DeadLetter

	wg sync.WaitGroup
}

// New monta o dispatcher.
func New(cfg Config, q *queue.Queue, m *metrics.Metrics, client *http.Client, log *slog.Logger) *Dispatcher {
	if cfg.Workers <= 0 {
		cfg.Workers = 1
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 1
	}
	if cfg.DLQCapacity <= 0 {
		cfg.DLQCapacity = 1000
	}
	return &Dispatcher{
		cfg:    cfg,
		q:      q,
		m:      m,
		client: client,
		log:    log,
		lim:    ratelimit.New(cfg.RatePerDestination, cfg.Burst),
	}
}

// Start sobe os workers. Cada um le da MESMA fila; o runtime distribui
// os itens entre eles. Nao ha divisao previa de trabalho, entao um
// worker preso num destino lento nao segura os eventos dos outros.
func (d *Dispatcher) Start(ctx context.Context) {
	d.lim.StartGC(ctx, 10*time.Minute)
	for i := 0; i < d.cfg.Workers; i++ {
		d.wg.Add(1)
		go d.worker(ctx, i)
	}
}

// Wait bloqueia ate todos os workers terminarem. Eles terminam quando a
// fila e fechada E esvaziada - um range sobre channel fechado so para
// depois de drenar o buffer, o que da o encerramento gracioso.
func (d *Dispatcher) Wait() { d.wg.Wait() }

func (d *Dispatcher) worker(ctx context.Context, id int) {
	defer d.wg.Done()
	for e := range d.q.C() {
		if ctx.Err() != nil {
			d.m.Dropped.Add(1)
			continue
		}
		d.m.InFlight.Add(1)
		d.deliver(ctx, e)
		d.m.InFlight.Add(-1)
	}
	d.log.Debug("worker encerrado", "worker", id)
}

// deliver executa o ciclo completo de tentativas para um evento.
func (d *Dispatcher) deliver(ctx context.Context, e event.Event) {
	start := time.Now()
	key := destinationKey(e.Destination)

	var lastErr error
	var lastStatus int
	attempt := 0

	for attempt = 1; attempt <= d.cfg.MaxAttempts; attempt++ {
		if attempt > 1 {
			d.m.Retries.Add(1)
			if !sleepCtx(ctx, d.backoff(attempt)) {
				lastErr = context.Canceled
				break
			}
		}

		if err := d.lim.Wait(ctx, key); err != nil {
			lastErr = err
			break
		}

		status, err := d.attempt(ctx, e, attempt)
		lastStatus, lastErr = status, err

		if err == nil && status >= 200 && status < 300 {
			d.m.Delivered.Add(1)
			d.m.Delivery.ObserveMicros(float64(time.Since(start).Microseconds()))
			return
		}
		if !retriable(status, err) {
			break
		}
	}

	// Ao sair do laco pela condicao, attempt ja foi incrementado alem
	// do limite. Registrar isso na DLQ reportaria uma tentativa que
	// nunca aconteceu.
	if attempt > d.cfg.MaxAttempts {
		attempt = d.cfg.MaxAttempts
	}

	msg := ""
	if lastErr != nil {
		msg = lastErr.Error()
	}
	d.deadLetter(DeadLetter{
		Event: e, Attempts: attempt, LastStatus: lastStatus,
		LastError: msg, At: time.Now(),
	})
}

// attempt faz uma unica requisicao HTTP ao destino.
func (d *Dispatcher) attempt(ctx context.Context, e event.Event, n int) (int, error) {
	// Timeout POR TENTATIVA, nao para o ciclo todo. Um destino que
	// aceita a conexao e nunca responde seguraria um worker para
	// sempre sem isto.
	reqCtx, cancel := context.WithTimeout(ctx, d.cfg.DeliveryTimeout)
	defer cancel()

	body := e.Payload
	if len(body) == 0 {
		body = []byte("{}")
	}

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, e.Destination, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "webhook-plataform/1.0")
	req.Header.Set("X-Event-Id", e.ID)
	req.Header.Set("X-Event-Type", e.Type)
	req.Header.Set("X-Delivery-Attempt", fmt.Sprint(n))

	resp, err := d.client.Do(req)
	if err != nil {
		return 0, err
	}
	// Drenar e fechar o corpo e obrigatorio para a conexao voltar ao
	// pool e ser reaproveitada. Sem isto, cada entrega abre um socket
	// novo e o custo de handshake domina o benchmark.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	_ = resp.Body.Close()

	return resp.StatusCode, nil
}

// backoff calcula a espera antes da tentativa n, com "full jitter".
//
// O jitter nao e detalhe: sem ele, mil eventos que falharam juntos
// (porque o destino caiu) voltariam juntos, exatamente no mesmo
// instante, derrubando o destino de novo assim que ele se levantasse.
// Sortear no intervalo [0, backoff] espalha as retentativas.
func (d *Dispatcher) backoff(attempt int) time.Duration {
	base := float64(d.cfg.BackoffBase)
	if base <= 0 {
		base = float64(100 * time.Millisecond)
	}
	max := float64(d.cfg.BackoffMax)
	if max <= 0 {
		max = float64(5 * time.Second)
	}
	d0 := base * math.Pow(2, float64(attempt-2))
	if d0 > max {
		d0 = max
	}
	return time.Duration(rand.Float64() * d0)
}

// retriable decide se vale tentar de novo.
//
// A regra segue a causa da falha: erro de rede e timeout sao
// transitorios; 429 e "desacelere"; 5xx e problema do outro lado.
// Ja um 4xx (fora 429) significa que a requisicao esta errada -
// repeti-la daria exatamente o mesmo erro, gastando recurso a toa.
func retriable(status int, err error) bool {
	if errors.Is(err, context.Canceled) {
		return false
	}
	if err != nil {
		return true
	}
	if status == http.StatusTooManyRequests || status == http.StatusRequestTimeout {
		return true
	}
	return status >= 500
}

func (d *Dispatcher) deadLetter(dl DeadLetter) {
	d.m.DeadLettered.Add(1)
	d.log.Warn("evento para a DLQ",
		"id", dl.Event.ID, "type", dl.Event.Type,
		"attempts", dl.Attempts, "status", dl.LastStatus, "erro", dl.LastError)

	d.dlqMu.Lock()
	defer d.dlqMu.Unlock()
	d.dlq = append(d.dlq, dl)
	// A DLQ e limitada: guardar tudo em memoria transformaria uma
	// indisponibilidade do destino num estouro de memoria aqui.
	if len(d.dlq) > d.cfg.DLQCapacity {
		d.dlq = d.dlq[len(d.dlq)-d.cfg.DLQCapacity:]
	}
}

// DLQ devolve uma copia das cartas mortas.
func (d *Dispatcher) DLQ() []DeadLetter {
	d.dlqMu.Lock()
	defer d.dlqMu.Unlock()
	out := make([]DeadLetter, len(d.dlq))
	copy(out, d.dlq)
	return out
}

func destinationKey(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return raw
	}
	return u.Scheme + "://" + u.Host
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
