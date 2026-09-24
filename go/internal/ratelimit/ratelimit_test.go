package ratelimit

import (
	"context"
	"testing"
	"time"
)

func TestDesabilitadoNaoEspera(t *testing.T) {
	l := New(0, 1)
	if l.Enabled() {
		t.Fatal("rate 0 deveria desabilitar")
	}
	start := time.Now()
	for i := 0; i < 100; i++ {
		if err := l.Wait(context.Background(), "d"); err != nil {
			t.Fatal(err)
		}
	}
	if d := time.Since(start); d > 50*time.Millisecond {
		t.Fatalf("sem limite nao deveria esperar, esperou %v", d)
	}
}

func TestRajadaPassaImediatamente(t *testing.T) {
	l := New(10, 5)
	start := time.Now()
	for i := 0; i < 5; i++ {
		if err := l.Wait(context.Background(), "d"); err != nil {
			t.Fatal(err)
		}
	}
	if d := time.Since(start); d > 50*time.Millisecond {
		t.Fatalf("a rajada de 5 deveria passar direto, levou %v", d)
	}
}

func TestLimitaTaxaAposRajada(t *testing.T) {
	// 20/s, rajada 1: a 2a chamada deve esperar ~50ms.
	l := New(20, 1)
	_ = l.Wait(context.Background(), "d")
	start := time.Now()
	_ = l.Wait(context.Background(), "d")
	d := time.Since(start)
	if d < 30*time.Millisecond {
		t.Fatalf("deveria ter esperado ~50ms, esperou %v", d)
	}
}

// O limite e POR DESTINO: um destino saturado nao pode travar os outros.
func TestDestinosSaoIndependentes(t *testing.T) {
	l := New(1, 1)
	_ = l.Wait(context.Background(), "destino-a")

	start := time.Now()
	if err := l.Wait(context.Background(), "destino-b"); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 50*time.Millisecond {
		t.Fatalf("outro destino nao deveria esperar, esperou %v", d)
	}
	if l.Keys() != 2 {
		t.Fatalf("esperava 2 baldes, veio %d", l.Keys())
	}
}

func TestRespeitaCancelamentoDoContexto(t *testing.T) {
	l := New(1, 1)
	_ = l.Wait(context.Background(), "d")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	start := time.Now()
	if err := l.Wait(ctx, "d"); err == nil {
		t.Fatal("esperava erro de contexto cancelado")
	}
	if d := time.Since(start); d > 200*time.Millisecond {
		t.Fatalf("deveria abortar rapido ao cancelar, levou %v", d)
	}
}
