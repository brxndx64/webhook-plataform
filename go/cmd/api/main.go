// Comando api: a plataforma de webhooks em Go.
//
//	go run ./cmd/api -workers 64 -queue 20000
//
// Dashboard ao vivo em http://localhost:8080/dashboard
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/brxndx64/webhook-plataform/go/internal/api"
	"github.com/brxndx64/webhook-plataform/go/internal/dedup"
	"github.com/brxndx64/webhook-plataform/go/internal/dispatcher"
	"github.com/brxndx64/webhook-plataform/go/internal/event"
	"github.com/brxndx64/webhook-plataform/go/internal/metrics"
	"github.com/brxndx64/webhook-plataform/go/internal/queue"
	"github.com/brxndx64/webhook-plataform/go/internal/safedial"
)

func main() {
	var (
		addr         = flag.String("addr", ":8080", "endereco de escuta")
		workers      = flag.Int("workers", runtime.NumCPU()*8, "numero de workers de entrega")
		queueSize    = flag.Int("queue", 20000, "capacidade da fila em memoria")
		maxAttempts  = flag.Int("retries", 3, "tentativas por evento (1 = sem retry)")
		backoffBase  = flag.Duration("backoff", 100*time.Millisecond, "backoff inicial")
		backoffMax   = flag.Duration("backoff-max", 5*time.Second, "backoff maximo")
		delTimeout   = flag.Duration("delivery-timeout", 5*time.Second, "timeout por tentativa de entrega")
		ratePerDest  = flag.Float64("rate", 0, "limite de entregas/s por destino (0 = sem limite)")
		burst        = flag.Int("burst", 50, "rajada permitida pelo rate limit")
		dedupTTL     = flag.Duration("dedup-ttl", 10*time.Minute, "janela de idempotencia")
		allowPrivate = flag.Bool("allow-private", false, "permitir destinos em rede interna (use apenas em benchmark)")
		dlqCap       = flag.Int("dlq", 1000, "capacidade da dead-letter queue")
		logLevel     = flag.String("log", "info", "debug|info|warn|error")
		maxBody      = flag.Int64("max-body", 1<<20, "tamanho maximo do corpo em bytes")
		shutdownWait = flag.Duration("shutdown-wait", 15*time.Second, "tempo para drenar a fila no encerramento")
	)
	flag.Parse()

	logger := newLogger(*logLevel)

	// signal.NotifyContext transforma Ctrl+C em cancelamento de
	// contexto, que se propaga por todo o sistema.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	m := metrics.New("go", *workers)
	q := queue.New(*queueSize)
	dd := dedup.New(*dedupTTL)
	dd.StartGC(ctx, time.Minute)

	m.QueueLen = q.Len
	m.QueueCap = q.Cap
	m.DedupSize = dd.Len

	client := newHTTPClient(*workers, *delTimeout, *allowPrivate)

	disp := dispatcher.New(dispatcher.Config{
		Workers:            *workers,
		MaxAttempts:        *maxAttempts,
		BackoffBase:        *backoffBase,
		BackoffMax:         *backoffMax,
		DeliveryTimeout:    *delTimeout,
		RatePerDestination: *ratePerDest,
		Burst:              *burst,
		DLQCapacity:        *dlqCap,
	}, q, m, client, logger)
	disp.Start(ctx)

	handler := api.NewHandler(api.Deps{
		Validator:    event.NewValidator(nil, nil, *allowPrivate, 64*1024),
		Queue:        q,
		Dedup:        dd,
		Metrics:      m,
		Dispatcher:   disp,
		Logger:       logger,
		MaxBodyBytes: *maxBody,
	})

	// Timeouts explicitos. http.ListenAndServe(addr, nil) nao define
	// nenhum: um cliente pode abrir conexao, enviar um byte por minuto
	// e segurar recursos indefinidamente (ataque Slowloris).
	srv := &http.Server{
		Addr:              *addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		logger.Info("servidor no ar",
			"addr", *addr, "workers", *workers, "fila", *queueSize,
			"retries", *maxAttempts, "rate_por_destino", *ratePerDest,
			"dashboard", "http://localhost"+normalizeAddr(*addr)+"/dashboard")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("falha ao escutar", "erro", err)
			stop()
		}
	}()

	<-ctx.Done()
	logger.Info("encerrando: parando de aceitar novos eventos")

	// Ordem do encerramento gracioso:
	// 1) parar o HTTP (nada novo entra)
	// 2) fechar a fila (workers drenam o que sobrou)
	// 3) esperar os workers, com prazo
	shutCtx, cancel := context.WithTimeout(context.Background(), *shutdownWait)
	defer cancel()
	_ = srv.Shutdown(shutCtx)

	q.Close()

	done := make(chan struct{})
	go func() { disp.Wait(); close(done) }()

	select {
	case <-done:
		logger.Info("fila drenada")
	case <-shutCtx.Done():
		logger.Warn("prazo esgotado; eventos restantes foram descartados",
			"na_fila", q.Len())
	}

	s := m.Snapshot()
	logger.Info("resumo",
		"recebidos", s.Received, "aceitos", s.Accepted,
		"entregues", s.Delivered, "dlq", s.DeadLettered,
		"api_p99_ms", s.APILatencyMs.P99Ms)
}

// newHTTPClient monta o cliente de saida.
//
// O pool de conexoes e o ponto critico: sem MaxIdleConnsPerHost alto,
// o Go mantem so 2 conexoes ociosas por host e reabre socket a cada
// entrega. O custo de handshake passa a dominar, e o benchmark mede
// o pool, nao a plataforma.
func newHTTPClient(workers int, timeout time.Duration, allowPrivate bool) *http.Client {
	idle := workers * 2
	if idle < 100 {
		idle = 100
	}
	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   3 * time.Second,
			KeepAlive: 30 * time.Second,
			Control:   safedial.Control(allowPrivate),
		}).DialContext,
		MaxIdleConns:          idle * 2,
		MaxIdleConnsPerHost:   idle,
		MaxConnsPerHost:       0,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: time.Second,
		ForceAttemptHTTP2:     true,
	}
	return &http.Client{Transport: tr, Timeout: timeout + time.Second}
}

func newLogger(level string) *slog.Logger {
	var l slog.Level
	switch strings.ToLower(level) {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: l}))
}

func normalizeAddr(a string) string {
	if strings.HasPrefix(a, ":") {
		return a
	}
	if i := strings.LastIndex(a, ":"); i >= 0 {
		return a[i:]
	}
	return ":" + a
}
