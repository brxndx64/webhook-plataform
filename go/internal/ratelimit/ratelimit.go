// Package ratelimit limita a taxa de entregas POR DESTINO.
//
// O limite e por destino, e nao global, porque o recurso escasso e o
// servidor do cliente. Um limite global faria um destino lento roubar
// a capacidade de todos os outros; um destino que aguenta 10 req/s nao
// deve receber 1000 so porque a sua plataforma consegue produzir.
package ratelimit

import (
	"context"
	"sync"
	"time"
)

type bucket struct {
	tokens float64
	last   time.Time
}

// Limiter e um conjunto de baldes de fichas (token bucket), um por chave.
//
// O balde enche a uma taxa constante ate um teto (burst) e cada entrega
// consome uma ficha. O teto e o que permite absorver uma rajada curta
// sem estourar a media no longo prazo.
type Limiter struct {
	rate  float64 // fichas por segundo; <= 0 desliga o limite
	burst float64

	mu      sync.Mutex
	buckets map[string]*bucket
}

// New cria o limitador. rate <= 0 desabilita a limitacao.
func New(rate float64, burst int) *Limiter {
	if burst <= 0 {
		burst = 1
	}
	return &Limiter{
		rate:    rate,
		burst:   float64(burst),
		buckets: make(map[string]*bucket),
	}
}

// Enabled diz se ha limitacao ativa.
func (l *Limiter) Enabled() bool { return l.rate > 0 }

// reserve consome uma ficha e devolve quanto tempo e preciso esperar.
//
// O saldo pode ficar NEGATIVO de proposito: isso reserva capacidade
// futura. Sem isso, varios workers concorrentes veriam "zero fichas",
// dormiriam o mesmo intervalo e acordariam juntos, criando uma
// estampida em vez de um fluxo constante.
func (l *Limiter) reserve(key string) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	b, ok := l.buckets[key]
	if !ok {
		b = &bucket{tokens: l.burst, last: now}
		l.buckets[key] = b
	}

	elapsed := now.Sub(b.last).Seconds()
	if elapsed > 0 {
		b.tokens += elapsed * l.rate
		if b.tokens > l.burst {
			b.tokens = l.burst
		}
		b.last = now
	}

	b.tokens--
	if b.tokens >= 0 {
		return 0
	}
	return time.Duration(-b.tokens / l.rate * float64(time.Second))
}

// Wait bloqueia ate haver ficha para a chave, ou ate o contexto ser
// cancelado. Respeitar o contexto e o que permite ao shutdown
// interromper um worker que esta apenas esperando sua vez.
func (l *Limiter) Wait(ctx context.Context, key string) error {
	if !l.Enabled() {
		return ctx.Err()
	}
	d := l.reserve(key)
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Keys conta quantos destinos distintos tem balde ativo.
func (l *Limiter) Keys() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.buckets)
}

// StartGC remove baldes ociosos para o mapa nao crescer sem limite
// quando ha muitos destinos distintos ao longo do tempo.
func (l *Limiter) StartGC(ctx context.Context, idle time.Duration) {
	if idle <= 0 {
		idle = 10 * time.Minute
	}
	go func() {
		t := time.NewTicker(idle)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				cutoff := time.Now().Add(-idle)
				l.mu.Lock()
				for k, b := range l.buckets {
					if b.last.Before(cutoff) {
						delete(l.buckets, k)
					}
				}
				l.mu.Unlock()
			}
		}
	}()
}
