// Package bench define o formato do resultado de um teste de carga.
// loadgen escreve, report le.
package bench

import (
	"time"

	"github.com/brxndx64/webhook-plataform/go/internal/metrics"
)

// Env registra onde o teste rodou. Sem isso o numero nao significa
// nada: 20k req/s num notebook e 20k req/s num servidor de 64 nucleos
// sao resultados completamente diferentes.
type Env struct {
	OS        string    `json:"os"`
	Arch      string    `json:"arch"`
	NumCPU    int       `json:"num_cpu"`
	Host      string    `json:"host"`
	GoVersion string    `json:"go_version"`
	When      time.Time `json:"when"`
}

// Process traz o custo de execucao do processo alvo, medido de fora
// (pelo script de benchmark). Medir de fora e o que permite comparar
// Go e Python na mesma unidade.
type Process struct {
	Name         string  `json:"name"`
	CPUSeconds   float64 `json:"cpu_seconds"`
	CPUPercent   float64 `json:"cpu_percent_of_one_core"`
	PeakRSSBytes int64   `json:"peak_rss_bytes"`
	AvgRSSBytes  int64   `json:"avg_rss_bytes"`
}

// Result e a saida de uma execucao do loadgen.
type Result struct {
	Label    string `json:"label"`
	Impl     string `json:"impl"`
	Scenario string `json:"scenario"`

	DurationSeconds float64 `json:"duration_seconds"`
	Concurrency     int     `json:"concurrency"`
	TargetRate      float64 `json:"target_rate"`

	Sent            int64            `json:"sent"`
	StatusCounts    map[string]int64 `json:"status_counts"`
	TransportErrors int64            `json:"transport_errors"`

	AttemptedRPS float64 `json:"attempted_rps"`
	AcceptedRPS  float64 `json:"accepted_rps"`

	ClientLatencyMs metrics.LatencyStats `json:"client_latency_ms"`

	DrainSeconds   float64 `json:"drain_seconds"`
	DrainComplete  bool    `json:"drain_complete"`
	DeliveredTotal int64   `json:"delivered_total"`
	DeliveryRPS    float64 `json:"delivery_rps"`

	Server  *metrics.Snapshot `json:"server,omitempty"`
	Process *Process          `json:"process,omitempty"`
	Env     Env               `json:"env"`
}
