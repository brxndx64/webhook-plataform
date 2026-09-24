package queue

import (
	"sync"
	"testing"

	"github.com/brxndx64/webhook-plataform/go/internal/event"
)

func TestEnqueueRespeitaCapacidade(t *testing.T) {
	q := New(2)
	for i := 0; i < 2; i++ {
		if err := q.Enqueue(event.Event{ID: "a"}); err != nil {
			t.Fatalf("insercao %d deveria caber: %v", i, err)
		}
	}
	if err := q.Enqueue(event.Event{ID: "estoura"}); err != ErrFull {
		t.Fatalf("esperava ErrFull, veio %v", err)
	}
	if q.Len() != 2 {
		t.Fatalf("esperava 2 itens, veio %d", q.Len())
	}
}

// Enqueue nao pode bloquear: bloquear aqui seguraria a goroutine do
// handler HTTP e derrubaria a latencia da API sob carga.
func TestEnqueueNaoBloqueiaComFilaCheia(t *testing.T) {
	q := New(1)
	_ = q.Enqueue(event.Event{ID: "1"})

	done := make(chan error, 1)
	go func() { done <- q.Enqueue(event.Event{ID: "2"}) }()

	select {
	case err := <-done:
		if err != ErrFull {
			t.Fatalf("esperava ErrFull, veio %v", err)
		}
	case <-make(chan struct{}):
	}
}

func TestCloseDrenaOQueSobrou(t *testing.T) {
	q := New(10)
	for i := 0; i < 5; i++ {
		_ = q.Enqueue(event.Event{ID: "x"})
	}
	q.Close()

	n := 0
	for range q.C() {
		n++
	}
	if n != 5 {
		t.Fatalf("esperava drenar 5 itens apos Close, veio %d", n)
	}
}

func TestEnqueueAposCloseFalha(t *testing.T) {
	q := New(2)
	q.Close()
	if err := q.Enqueue(event.Event{ID: "x"}); err != ErrClosed {
		t.Fatalf("esperava ErrClosed, veio %v", err)
	}
}

func TestCloseDuplicadoNaoEntraEmPanico(t *testing.T) {
	q := New(1)
	q.Close()
	q.Close()
}

// Rode com -race para valer.
func TestEnqueueConcorrente(t *testing.T) {
	q := New(1000)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				_ = q.Enqueue(event.Event{ID: "x"})
			}
		}()
	}
	wg.Wait()
	if q.Len() != 1000 {
		t.Fatalf("esperava 1000 itens, veio %d", q.Len())
	}
}
