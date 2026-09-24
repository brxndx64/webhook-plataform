package metrics

import (
	"math"
	"sync/atomic"
)

// O histograma usa baldes geometricos: cada balde e 6% maior que o
// anterior. Isso da erro relativo maximo de ~6% em qualquer percentil,
// com memoria constante (253 contadores), independente de quantas
// amostras entrarem.
//
// A alternativa - guardar todas as amostras e ordenar - daria
// percentil exato, mas alocaria memoria proporcional ao numero de
// requisicoes. Num teste de carga de 1 milhao de eventos isso
// distorceria a propria medicao, que e justamente o que queremos medir.
const (
	minMicros  = 50.0
	factor     = 1.06
	numBuckets = 253
)

var logFactor = math.Log(factor)

// Histogram acumula latencias em microssegundos, sem lock.
type Histogram struct {
	buckets [numBuckets]atomic.Int64
	count   atomic.Int64
	sumUS   atomic.Int64
	maxUS   atomic.Int64
}

func NewHistogram() *Histogram { return &Histogram{} }

func bucketIndex(us float64) int {
	if us < minMicros {
		return 0
	}
	i := int(math.Log(us/minMicros)/logFactor) + 1
	if i >= numBuckets {
		return numBuckets - 1
	}
	if i < 0 {
		return 0
	}
	return i
}

func upperBoundUS(i int) float64 {
	if i <= 0 {
		return minMicros
	}
	return minMicros * math.Pow(factor, float64(i))
}

// ObserveMicros registra uma amostra.
func (h *Histogram) ObserveMicros(us float64) {
	if us < 0 {
		us = 0
	}
	h.buckets[bucketIndex(us)].Add(1)
	h.count.Add(1)
	h.sumUS.Add(int64(us))

	iu := int64(us)
	for {
		cur := h.maxUS.Load()
		if iu <= cur || h.maxUS.CompareAndSwap(cur, iu) {
			break
		}
	}
}

// LatencyStats e o resumo em milissegundos, pronto para JSON.
type LatencyStats struct {
	Count  int64   `json:"count"`
	MeanMs float64 `json:"mean_ms"`
	P50Ms  float64 `json:"p50_ms"`
	P95Ms  float64 `json:"p95_ms"`
	P99Ms  float64 `json:"p99_ms"`
	MaxMs  float64 `json:"max_ms"`
}

// Stats calcula o resumo.
//
// Percentis, e nao media: a media esconde a cauda. Num sistema com
// 99% das respostas em 1ms e 1% em 2s, a media fica em ~21ms e parece
// otima, enquanto um cliente a cada cem espera dois segundos. O p99 e
// o numero que revela isso.
func (h *Histogram) Stats() LatencyStats {
	total := h.count.Load()
	s := LatencyStats{Count: total}
	if total == 0 {
		return s
	}
	s.MeanMs = float64(h.sumUS.Load()) / float64(total) / 1000.0
	s.MaxMs = float64(h.maxUS.Load()) / 1000.0

	counts := make([]int64, numBuckets)
	for i := range counts {
		counts[i] = h.buckets[i].Load()
	}

	q := func(p float64) float64 {
		target := p * float64(total)
		var cum int64
		for i := 0; i < numBuckets; i++ {
			cum += counts[i]
			if float64(cum) >= target {
				return upperBoundUS(i) / 1000.0
			}
		}
		return s.MaxMs
	}

	s.P50Ms = q(0.50)
	s.P95Ms = q(0.95)
	s.P99Ms = q(0.99)

	// O histograma nunca pode reportar mais que o maximo real.
	if s.P99Ms > s.MaxMs {
		s.P99Ms = s.MaxMs
	}
	if s.P95Ms > s.MaxMs {
		s.P95Ms = s.MaxMs
	}
	if s.P50Ms > s.MaxMs {
		s.P50Ms = s.MaxMs
	}
	return s
}

// Reset zera o histograma.
func (h *Histogram) Reset() {
	for i := range h.buckets {
		h.buckets[i].Store(0)
	}
	h.count.Store(0)
	h.sumUS.Store(0)
	h.maxUS.Store(0)
}
