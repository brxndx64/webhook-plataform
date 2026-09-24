package dedup

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPrimeiraVezNaoEDuplicata(t *testing.T) {
	s := New(time.Minute)
	if s.CheckAndMark("evt_1") {
		t.Fatal("primeira ocorrencia nao pode ser duplicata")
	}
	if !s.CheckAndMark("evt_1") {
		t.Fatal("segunda ocorrencia deveria ser duplicata")
	}
}

func TestIDsDiferentesNaoColidem(t *testing.T) {
	s := New(time.Minute)
	for i := 0; i < 1000; i++ {
		if s.CheckAndMark(fmt.Sprintf("evt_%d", i)) {
			t.Fatalf("evt_%d marcado como duplicata sem ter sido visto", i)
		}
	}
	if s.Len() != 1000 {
		t.Fatalf("esperava 1000 ids, veio %d", s.Len())
	}
}

func TestExpiraDepoisDoTTL(t *testing.T) {
	s := New(50 * time.Millisecond)
	s.CheckAndMark("evt_1")
	time.Sleep(80 * time.Millisecond)
	if s.CheckAndMark("evt_1") {
		t.Fatal("apos o TTL o id deveria ser tratado como novo")
	}
}

// O cenario que a idempotencia existe para resolver: a resposta se
// perde e o cliente reenvia o MESMO evento. Apenas uma passa.
func TestApenasUmaPassaSobConcorrencia(t *testing.T) {
	s := New(time.Minute)
	const n = 200
	var passaram atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if !s.CheckAndMark("evt_repetido") {
				passaram.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := passaram.Load(); got != 1 {
		t.Fatalf("exatamente 1 deveria passar, passaram %d", got)
	}
}

func TestGCRemoveExpirados(t *testing.T) {
	s := New(20 * time.Millisecond)
	for i := 0; i < 100; i++ {
		s.CheckAndMark(fmt.Sprintf("e%d", i))
	}
	time.Sleep(40 * time.Millisecond)
	s.collect()
	if s.Len() != 0 {
		t.Fatalf("esperava 0 apos a coleta, veio %d", s.Len())
	}
}
