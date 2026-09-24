package metrics

import (
	"math"
	"testing"
)

func TestHistogramaVazio(t *testing.T) {
	h := NewHistogram()
	s := h.Stats()
	if s.Count != 0 || s.P99Ms != 0 {
		t.Fatalf("histograma vazio deveria zerar tudo, veio %+v", s)
	}
}

// Com 100 amostras de 1 a 100ms, o p50 fica ~50ms e o p99 ~99ms.
// A tolerancia de 10% cobre o erro de ~6% dos baldes geometricos.
func TestPercentisAproximados(t *testing.T) {
	h := NewHistogram()
	for i := 1; i <= 100; i++ {
		h.ObserveMicros(float64(i) * 1000)
	}
	s := h.Stats()

	if s.Count != 100 {
		t.Fatalf("esperava 100 amostras, veio %d", s.Count)
	}
	check := func(nome string, got, want float64) {
		if math.Abs(got-want)/want > 0.10 {
			t.Errorf("%s = %.2f ms, esperado ~%.2f ms (fora da tolerancia de 10%%)", nome, got, want)
		}
	}
	check("p50", s.P50Ms, 50)
	check("p95", s.P95Ms, 95)
	check("p99", s.P99Ms, 99)
	check("media", s.MeanMs, 50.5)

	if s.MaxMs < 99 || s.MaxMs > 101 {
		t.Errorf("max = %.2f, esperado 100", s.MaxMs)
	}
}

// A razao de existir o p99: a media esconde a cauda.
func TestP99RevelaACaudaQueAMediaEsconde(t *testing.T) {
	// 2% na cauda, e nao exatamente 1%: com 1% cravado, o p99 cai
	// em cima da fronteira entre os dois grupos e qualquer definicao
	// de percentil fica ambigua. O teste mediria o criterio de
	// desempate, nao o comportamento.
	h := NewHistogram()
	for i := 0; i < 980; i++ {
		h.ObserveMicros(1000) // 1ms
	}
	for i := 0; i < 20; i++ {
		h.ObserveMicros(2_000_000) // 2s
	}
	s := h.Stats()

	if s.MeanMs > 50 {
		t.Fatalf("cenario montado errado: media %.1f", s.MeanMs)
	}
	if s.P50Ms > 2 {
		t.Errorf("p50 deveria ficar em ~1ms, veio %.2f", s.P50Ms)
	}
	if s.P99Ms < 1000 {
		t.Errorf("p99 deveria revelar a cauda de 2s, veio %.2f ms", s.P99Ms)
	}
}

func TestPercentilNuncaPassaDoMaximo(t *testing.T) {
	h := NewHistogram()
	h.ObserveMicros(1500)
	s := h.Stats()
	if s.P99Ms > s.MaxMs {
		t.Fatalf("p99 (%.3f) nao pode exceder o max (%.3f)", s.P99Ms, s.MaxMs)
	}
}

func TestResetZera(t *testing.T) {
	h := NewHistogram()
	for i := 0; i < 50; i++ {
		h.ObserveMicros(5000)
	}
	h.Reset()
	if s := h.Stats(); s.Count != 0 || s.MaxMs != 0 {
		t.Fatalf("apos Reset deveria zerar, veio %+v", s)
	}
}

func TestValoresExtremos(t *testing.T) {
	h := NewHistogram()
	h.ObserveMicros(0)
	h.ObserveMicros(1)
	h.ObserveMicros(1e9) // 1000s, acima do ultimo balde
	if s := h.Stats(); s.Count != 3 {
		t.Fatalf("esperava 3 amostras, veio %d", s.Count)
	}
}

func BenchmarkObserve(b *testing.B) {
	h := NewHistogram()
	b.ReportAllocs()
	b.RunParallel(func(p *testing.PB) {
		for p.Next() {
			h.ObserveMicros(1234)
		}
	})
}
