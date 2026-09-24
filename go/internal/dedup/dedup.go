// Package dedup implementa a idempotencia: reconhecer um evento que
// ja foi aceito e nao processa-lo de novo.
//
// O cenario que isto resolve: o cliente envia "cobrar R$ 1500", a API
// aceita e responde 202, mas a resposta se perde na rede. O cliente
// nao recebeu nada e reenvia. Sem deduplicacao o cliente e cobrado
// duas vezes. Como o ID vem do cliente, a repeticao chega com a mesma
// identidade e pode ser reconhecida.
package dedup

import (
	"context"
	"hash/fnv"
	"sync"
	"time"
)

// shardCount e o numero de fatias do mapa. Um unico mapa com um unico
// mutex vira ponto de contencao: com 64 workers, todos disputariam o
// mesmo lock a cada evento. Dividir por hash do ID espalha a disputa.
const shardCount = 64

type shard struct {
	mu   sync.Mutex
	seen map[string]time.Time
}

// Store guarda os IDs vistos por uma janela de tempo.
//
// Em producao isto seria Redis (a memoria nao sobrevive a um restart
// e nao e compartilhada entre instancias). Em memoria e suficiente
// para este projeto e mantem a comparacao Go/Python justa, ja que as
// duas implementacoes usam a mesma estrategia.
type Store struct {
	ttl    time.Duration
	shards [shardCount]*shard
}

// New cria o store. O ttl define por quanto tempo um ID e lembrado.
func New(ttl time.Duration) *Store {
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}
	s := &Store{ttl: ttl}
	for i := range s.shards {
		s.shards[i] = &shard{seen: make(map[string]time.Time)}
	}
	return s
}

func (s *Store) shardFor(id string) *shard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	return s.shards[h.Sum32()%shardCount]
}

// CheckAndMark marca o ID como visto e devolve true se ele JA era
// conhecido. As duas coisas acontecem sob o mesmo lock - checar e
// marcar em passos separados abriria uma janela em que dois workers
// concorrentes veriam "nao visto" para o mesmo ID.
func (s *Store) CheckAndMark(id string) bool {
	sh := s.shardFor(id)
	now := time.Now()

	sh.mu.Lock()
	defer sh.mu.Unlock()

	if at, ok := sh.seen[id]; ok && now.Sub(at) < s.ttl {
		return true
	}
	sh.seen[id] = now
	return false
}

// Unmark desfaz a marcacao de um ID.
//
// Necessario quando o evento e marcado mas NAO chega a ser aceito -
// por exemplo, quando a fila esta cheia e a API devolve 503. Sem isto,
// o ID ficaria marcado como visto, e a retentativa que o proprio
// Retry-After pediu seria descartada como duplicata: o evento se
// perderia em silencio.
func (s *Store) Unmark(id string) {
	sh := s.shardFor(id)
	sh.mu.Lock()
	delete(sh.seen, id)
	sh.mu.Unlock()
}

// Clear esvazia o store. Usado apenas pelo /admin/reset dos benchmarks.
func (s *Store) Clear() {
	for _, sh := range s.shards {
		sh.mu.Lock()
		sh.seen = make(map[string]time.Time)
		sh.mu.Unlock()
	}
}

// Len conta os IDs lembrados no momento.
func (s *Store) Len() int {
	n := 0
	for _, sh := range s.shards {
		sh.mu.Lock()
		n += len(sh.seen)
		sh.mu.Unlock()
	}
	return n
}

// StartGC roda a limpeza periodica ate o contexto ser cancelado.
// Sem isso o mapa cresce para sempre e vira um vazamento de memoria.
func (s *Store) StartGC(ctx context.Context, every time.Duration) {
	if every <= 0 {
		every = time.Minute
	}
	go func() {
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				s.collect()
			}
		}
	}()
}

func (s *Store) collect() {
	cutoff := time.Now().Add(-s.ttl)
	for _, sh := range s.shards {
		sh.mu.Lock()
		for id, at := range sh.seen {
			if at.Before(cutoff) {
				delete(sh.seen, id)
			}
		}
		sh.mu.Unlock()
	}
}
