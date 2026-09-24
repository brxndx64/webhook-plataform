// Package queue implementa a fila em memoria entre a API e os workers.
//
// A fila e um channel com buffer. O buffer e o que desacopla a API dos
// workers: a API responde 202 assim que enfileira, sem esperar a
// entrega. Mas o buffer e FINITO de proposito - uma fila ilimitada
// apenas troca "recusar agora" por "estourar a memoria daqui a pouco".
package queue

import (
	"errors"
	"sync"

	"github.com/brxndx64/webhook-plataform/go/internal/event"
)

// ErrFull indica que a fila esta cheia. A API traduz isso em
// 503 Service Unavailable com Retry-After: e backpressure, ou seja,
// dizer ao cliente para desacelerar em vez de aceitar trabalho que
// nao se consegue fazer.
var ErrFull = errors.New("fila cheia")

// ErrClosed indica enfileiramento apos o fechamento.
var ErrClosed = errors.New("fila fechada")

// Queue e uma fila FIFO limitada, segura para uso concorrente.
type Queue struct {
	ch chan event.Event

	mu     sync.RWMutex
	closed bool
}

// New cria a fila com a capacidade informada.
func New(capacity int) *Queue {
	if capacity <= 0 {
		capacity = 1
	}
	return &Queue{ch: make(chan event.Event, capacity)}
}

// Enqueue insere sem bloquear. Devolve ErrFull quando nao ha espaco.
//
// O select com default e o que torna a operacao nao bloqueante: se o
// envio no channel nao pode prosseguir imediatamente, cai no default
// em vez de esperar. Bloquear aqui seguraria a goroutine do handler
// HTTP e derrubaria a latencia da API sob carga.
func (q *Queue) Enqueue(e event.Event) error {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed {
		return ErrClosed
	}
	select {
	case q.ch <- e:
		return nil
	default:
		return ErrFull
	}
}

// C devolve o channel de leitura consumido pelos workers.
func (q *Queue) C() <-chan event.Event { return q.ch }

// Len e a quantidade de itens aguardando.
func (q *Queue) Len() int { return len(q.ch) }

// Cap e a capacidade total.
func (q *Queue) Cap() int { return cap(q.ch) }

// Close fecha a fila. Os workers terminam de drenar o que restou e
// entao encerram, porque um range sobre channel fechado para quando
// o buffer esvazia.
func (q *Queue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.closed = true
	close(q.ch)
}
