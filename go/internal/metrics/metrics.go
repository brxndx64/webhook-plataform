// Package metrics coleta os numeros que sustentam a comparacao Go x Python.
//
// Tudo aqui usa contadores atomicos em vez de mutex: a instrumentacao
// nao pode virar o gargalo que ela deveria medir.
package metrics

import (
	"fmt"
	"io"
	"runtime"
	"sync/atomic"
	"time"
)

// Metrics agrega todos os contadores do servico.
type Metrics struct {
	start atomic.Int64 // UnixNano

	// Entrada (API)
	Received         atomic.Int64
	Accepted         atomic.Int64
	Duplicated       atomic.Int64
	Invalid          atomic.Int64
	RejectedFull     atomic.Int64
	MethodNotAllowed atomic.Int64

	// Saida (workers)
	Delivered    atomic.Int64
	Retries      atomic.Int64
	DeadLettered atomic.Int64
	Dropped      atomic.Int64
	InFlight     atomic.Int64

	API      *Histogram
	Delivery *Histogram

	// Preenchidos pelo servidor a cada snapshot.
	QueueLen  func() int
	QueueCap  func() int
	DedupSize func() int
	Workers   int
	Impl      string
}

// New cria o coletor.
func New(impl string, workers int) *Metrics {
	m := &Metrics{
		API:      NewHistogram(),
		Delivery: NewHistogram(),
		Workers:  workers,
		Impl:     impl,
	}
	m.start.Store(time.Now().UnixNano())
	return m
}

// RuntimeStats descreve o custo de execucao do processo.
type RuntimeStats struct {
	Goroutines      int     `json:"goroutines"`
	HeapAllocBytes  uint64  `json:"heap_alloc_bytes"`
	HeapObjects     uint64  `json:"heap_objects"`
	TotalAllocBytes uint64  `json:"total_alloc_bytes"`
	MallocsTotal    uint64  `json:"mallocs_total"`
	NumGC           uint32  `json:"num_gc"`
	GCPauseTotalMs  float64 `json:"gc_pause_total_ms"`
	NumCPU          int     `json:"num_cpu"`
	GOMAXPROCS      int     `json:"gomaxprocs"`
	Version         string  `json:"version"`
}

// Snapshot e a foto instantanea, serializada em /stats.
// A implementacao Python devolve exatamente esta mesma forma, para que
// o mesmo dashboard e o mesmo relatorio sirvam para as duas.
type Snapshot struct {
	Impl          string  `json:"impl"`
	UptimeSeconds float64 `json:"uptime_seconds"`

	Received         int64 `json:"received"`
	Accepted         int64 `json:"accepted"`
	Duplicated       int64 `json:"duplicated"`
	Invalid          int64 `json:"invalid"`
	RejectedFull     int64 `json:"rejected_full"`
	MethodNotAllowed int64 `json:"method_not_allowed"`

	Delivered    int64 `json:"delivered"`
	Retries      int64 `json:"retries"`
	DeadLettered int64 `json:"dead_lettered"`
	Dropped      int64 `json:"dropped"`
	InFlight     int64 `json:"in_flight"`

	QueueLen  int `json:"queue_len"`
	QueueCap  int `json:"queue_cap"`
	DedupSize int `json:"dedup_size"`
	Workers   int `json:"workers"`

	APILatencyMs      LatencyStats `json:"api_latency_ms"`
	DeliveryLatencyMs LatencyStats `json:"delivery_latency_ms"`

	Runtime RuntimeStats `json:"runtime"`
}

// Snapshot monta a foto atual.
func (m *Metrics) Snapshot() Snapshot {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	s := Snapshot{
		Impl:          m.Impl,
		UptimeSeconds: time.Since(time.Unix(0, m.start.Load())).Seconds(),

		Received:         m.Received.Load(),
		Accepted:         m.Accepted.Load(),
		Duplicated:       m.Duplicated.Load(),
		Invalid:          m.Invalid.Load(),
		RejectedFull:     m.RejectedFull.Load(),
		MethodNotAllowed: m.MethodNotAllowed.Load(),

		Delivered:    m.Delivered.Load(),
		Retries:      m.Retries.Load(),
		DeadLettered: m.DeadLettered.Load(),
		Dropped:      m.Dropped.Load(),
		InFlight:     m.InFlight.Load(),

		Workers:           m.Workers,
		APILatencyMs:      m.API.Stats(),
		DeliveryLatencyMs: m.Delivery.Stats(),

		Runtime: RuntimeStats{
			Goroutines:      runtime.NumGoroutine(),
			HeapAllocBytes:  ms.HeapAlloc,
			HeapObjects:     ms.HeapObjects,
			TotalAllocBytes: ms.TotalAlloc,
			MallocsTotal:    ms.Mallocs,
			NumGC:           ms.NumGC,
			GCPauseTotalMs:  float64(ms.PauseTotalNs) / 1e6,
			NumCPU:          runtime.NumCPU(),
			GOMAXPROCS:      runtime.GOMAXPROCS(0),
			Version:         runtime.Version(),
		},
	}
	if m.QueueLen != nil {
		s.QueueLen = m.QueueLen()
	}
	if m.QueueCap != nil {
		s.QueueCap = m.QueueCap()
	}
	if m.DedupSize != nil {
		s.DedupSize = m.DedupSize()
	}
	return s
}

// Reset zera os contadores. Usado entre execucoes de benchmark.
func (m *Metrics) Reset() {
	m.start.Store(time.Now().UnixNano())
	for _, c := range []*atomic.Int64{
		&m.Received, &m.Accepted, &m.Duplicated, &m.Invalid,
		&m.RejectedFull, &m.MethodNotAllowed, &m.Delivered,
		&m.Retries, &m.DeadLettered, &m.Dropped,
	} {
		c.Store(0)
	}
	m.API.Reset()
	m.Delivery.Reset()
}

// WritePrometheus expoe as metricas no formato de texto do Prometheus,
// para quem quiser plugar Grafana em cima.
func (m *Metrics) WritePrometheus(w io.Writer) {
	s := m.Snapshot()
	p := func(name, help, typ string, v any) {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n%s %v\n", name, help, name, typ, name, v)
	}
	p("webhook_received_total", "Requisicoes recebidas em /notifications", "counter", s.Received)
	p("webhook_accepted_total", "Eventos aceitos (202)", "counter", s.Accepted)
	p("webhook_duplicated_total", "Eventos barrados por idempotencia", "counter", s.Duplicated)
	p("webhook_invalid_total", "Eventos rejeitados por validacao (400)", "counter", s.Invalid)
	p("webhook_rejected_full_total", "Eventos rejeitados por fila cheia (503)", "counter", s.RejectedFull)
	p("webhook_delivered_total", "Entregas bem-sucedidas", "counter", s.Delivered)
	p("webhook_retries_total", "Tentativas de reenvio", "counter", s.Retries)
	p("webhook_dead_lettered_total", "Eventos enviados para a DLQ", "counter", s.DeadLettered)
	p("webhook_queue_length", "Itens na fila", "gauge", s.QueueLen)
	p("webhook_queue_capacity", "Capacidade da fila", "gauge", s.QueueCap)
	p("webhook_in_flight", "Entregas em andamento", "gauge", s.InFlight)
	p("webhook_api_latency_p99_ms", "Latencia p99 da API em ms", "gauge", s.APILatencyMs.P99Ms)
	p("webhook_delivery_latency_p99_ms", "Latencia p99 da entrega em ms", "gauge", s.DeliveryLatencyMs.P99Ms)
	p("webhook_goroutines", "Goroutines vivas", "gauge", s.Runtime.Goroutines)
	p("webhook_heap_alloc_bytes", "Heap alocado", "gauge", s.Runtime.HeapAllocBytes)
	p("webhook_gc_total", "Ciclos de GC", "counter", s.Runtime.NumGC)
}
