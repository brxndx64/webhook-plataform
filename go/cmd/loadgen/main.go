// Comando loadgen: gerador de carga usado contra AS DUAS implementacoes.
//
// O mesmo gerador, o mesmo provedor simulado e os mesmos parametros
// para Go e para Python. Usar ferramentas diferentes para cada lado
// mediria as ferramentas, nao as implementacoes.
//
//	go run ./cmd/loadgen -target http://localhost:8080 -duration 30s -concurrency 64
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/brxndx64/webhook-plataform/go/internal/bench"
	"github.com/brxndx64/webhook-plataform/go/internal/metrics"
)

var eventTypes = []string{
	"payment.approved", "payment.refused", "order.created",
	"order.shipped", "delivery.updated",
}

// runID e um prefixo aleatorio para os IDs de evento.
//
// Sem ele, a fase de aquecimento e a fase medida gerariam a MESMA
// sequencia de IDs ("evt-0-1", "evt-0-2"...), e a janela de
// idempotencia do servidor trataria quase tudo da fase medida como
// duplicata - medindo o deduplicador em vez do pipeline.
var runID = strconv.FormatInt(time.Now().UnixNano(), 36)

func main() {
	var (
		target      = flag.String("target", "http://localhost:8080", "URL da API sob teste")
		dest        = flag.String("dest", "http://localhost:9000/hook", "destino do webhook (provedor simulado)")
		duration    = flag.Duration("duration", 30*time.Second, "duracao da fase de carga")
		warmup      = flag.Duration("warmup", 3*time.Second, "aquecimento antes de zerar as metricas")
		concurrency = flag.Int("concurrency", 64, "conexoes concorrentes")
		rate        = flag.Float64("rate", 0, "taxa alvo em req/s (0 = malha fechada, o mais rapido possivel)")
		payloadSize = flag.Int("payload", 256, "tamanho aproximado do payload em bytes")
		dupRate     = flag.Float64("dup-rate", 0.02, "fracao de eventos reenviados com o mesmo id")
		drainWait   = flag.Duration("drain-wait", 90*time.Second, "tempo maximo esperando a fila esvaziar")
		label       = flag.String("label", "", "rotulo do resultado (padrao: impl do servidor)")
		scenario    = flag.String("scenario", "default", "nome do cenario")
		out         = flag.String("out", "", "arquivo JSON de saida")
		quiet       = flag.Bool("quiet", false, "menos saida no terminal")
	)
	flag.Parse()

	client := newClient(*concurrency)

	// 1) Aquecimento. Python precisa disso mais que Go (import lento,
	// pools de conexao frios). Sem aquecer, os primeiros segundos
	// penalizariam injustamente quem demora mais para estabilizar.
	if *warmup > 0 {
		logf(*quiet, "aquecendo por %v...", *warmup)
		runPhase(client, *target, *dest, *warmup, min(*concurrency, 16), 0, *payloadSize, 0, nil, nil)

		// Esperar o aquecimento drenar ANTES de zerar. Sem isto, os
		// eventos do aquecimento ainda na fila seriam contados como
		// entregas da fase medida, inflando o resultado.
		logf(*quiet, "drenando o aquecimento...")
		waitDrain(client, *target, 60*time.Second)
	}

	// 2) Zerar as metricas do servidor para que so a fase medida conte.
	if err := resetTarget(client, *target); err != nil {
		log.Printf("aviso: nao foi possivel zerar as metricas do alvo: %v", err)
	}

	// 3) Fase medida.
	logf(*quiet, "carga: %v, concorrencia=%d, taxa=%s",
		*duration, *concurrency, rateLabel(*rate))

	hist := metrics.NewHistogram()
	var st stats
	start := time.Now()
	runPhase(client, *target, *dest, *duration, *concurrency, *rate, *payloadSize, *dupRate, hist, &st)
	sendElapsed := time.Since(start)

	// 4) Esperar a fila drenar. So o que foi ENTREGUE conta como
	// trabalho concluido; aceitar 100k eventos e entregar 3k nao e
	// desempenho, e uma fila crescendo.
	logf(*quiet, "aguardando a fila drenar...")
	drainStart := time.Now()
	snap, complete := waitDrain(client, *target, *drainWait)
	drainElapsed := time.Since(drainStart)

	r := bench.Result{
		Scenario:        *scenario,
		DurationSeconds: sendElapsed.Seconds(),
		Concurrency:     *concurrency,
		TargetRate:      *rate,
		Sent:            st.sent.Load(),
		StatusCounts:    st.snapshotStatuses(),
		TransportErrors: st.errs.Load(),
		ClientLatencyMs: hist.Stats(),
		DrainSeconds:    drainElapsed.Seconds(),
		DrainComplete:   complete,
		Server:          snap,
		Env: bench.Env{
			OS: runtime.GOOS, Arch: runtime.GOARCH,
			NumCPU: runtime.NumCPU(), Host: hostname(),
			GoVersion: runtime.Version(), When: time.Now(),
		},
	}
	r.AttemptedRPS = float64(r.Sent) / sendElapsed.Seconds()
	r.AcceptedRPS = float64(r.StatusCounts["202"]) / sendElapsed.Seconds()
	if snap != nil {
		r.Impl = snap.Impl
		r.DeliveredTotal = snap.Delivered
		total := sendElapsed.Seconds() + drainElapsed.Seconds()
		if total > 0 {
			r.DeliveryRPS = float64(snap.Delivered) / total
		}
	}
	r.Label = *label
	if r.Label == "" {
		r.Label = r.Impl
	}
	if r.Label == "" {
		r.Label = "desconhecido"
	}

	printSummary(r)

	if *out != "" {
		if err := writeResult(*out, r); err != nil {
			log.Fatalf("erro ao escrever %s: %v", *out, err)
		}
		fmt.Printf("\nresultado salvo em %s\n", *out)
	}
}

type stats struct {
	sent     atomic.Int64
	errs     atomic.Int64
	mu       sync.Mutex
	statuses map[int]int64
}

func (s *stats) addStatus(code int) {
	s.mu.Lock()
	if s.statuses == nil {
		s.statuses = make(map[int]int64)
	}
	s.statuses[code]++
	s.mu.Unlock()
}

func (s *stats) snapshotStatuses() map[string]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]int64, len(s.statuses))
	for k, v := range s.statuses {
		out[strconv.Itoa(k)] = v
	}
	return out
}

// runPhase executa uma fase de carga.
//
// Dois modos:
//   - malha fechada (rate=0): N conexoes disparam sem parar. Mede
//     capacidade maxima (vazao).
//   - malha aberta (rate>0): as chegadas sao agendadas a uma taxa fixa
//     e a latencia e medida a partir do INSTANTE AGENDADO, nao do
//     instante em que a requisicao conseguiu sair. Isso corrige a
//     "omissao coordenada": se o servidor trava 1s, um gerador de malha
//     fechada simplesmente para de enviar e nunca registra a espera,
//     reportando uma latencia otimista e falsa.
func runPhase(client *http.Client, target, dest string, dur time.Duration,
	conc int, rate float64, payloadSize int, dupRate float64,
	hist *metrics.Histogram, st *stats) {

	url := target + "/notifications"
	ctx, cancel := context.WithTimeout(context.Background(), dur)
	defer cancel()

	payload := makePayload(payloadSize)
	var wg sync.WaitGroup

	if rate <= 0 {
		for i := 0; i < conc; i++ {
			wg.Add(1)
			go func(worker int) {
				defer wg.Done()
				g := newGen(worker, dest, payload, dupRate)
				for ctx.Err() == nil {
					t0 := time.Now()
					code, err := send(ctx, client, url, g.next())
					record(hist, st, t0, code, err)
				}
			}(i)
		}
		wg.Wait()
		return
	}

	// Malha aberta: um agendador distribui instantes de chegada.
	sched := make(chan time.Time, conc*4)
	go func() {
		defer close(sched)
		interval := time.Duration(float64(time.Second) / rate)
		next := time.Now()
		for ctx.Err() == nil {
			next = next.Add(interval)
			if d := time.Until(next); d > 0 {
				timer := time.NewTimer(d)
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
			select {
			case sched <- next:
			case <-ctx.Done():
				return
			}
		}
	}()

	for i := 0; i < conc; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			g := newGen(worker, dest, payload, dupRate)
			for due := range sched {
				code, err := send(context.Background(), client, url, g.next())
				record(hist, st, due, code, err)
			}
		}(i)
	}
	wg.Wait()
}

func record(hist *metrics.Histogram, st *stats, since time.Time, code int, err error) {
	if hist != nil {
		hist.ObserveMicros(float64(time.Since(since).Microseconds()))
	}
	if st == nil {
		return
	}
	st.sent.Add(1)
	if err != nil {
		st.errs.Add(1)
		return
	}
	st.addStatus(code)
}

func send(ctx context.Context, client *http.Client, url string, body []byte) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return resp.StatusCode, nil
}

// gen monta os corpos das requisicoes reaproveitando um buffer, para
// que o proprio gerador nao vire o gargalo do teste.
type gen struct {
	buf     []byte
	worker  int
	n       int64
	dest    string
	payload []byte
	dupRate float64
	lastID  []byte
}

func newGen(worker int, dest string, payload []byte, dupRate float64) *gen {
	return &gen{
		buf: make([]byte, 0, 1024), worker: worker,
		dest: dest, payload: payload, dupRate: dupRate,
		lastID: make([]byte, 0, 32),
	}
}

func (g *gen) next() []byte {
	g.n++
	reuse := g.dupRate > 0 && len(g.lastID) > 0 && rand.Float64() < g.dupRate
	if !reuse {
		g.lastID = g.lastID[:0]
		g.lastID = append(g.lastID, "evt-"...)
		g.lastID = append(g.lastID, runID...)
		g.lastID = append(g.lastID, '-')
		g.lastID = strconv.AppendInt(g.lastID, int64(g.worker), 10)
		g.lastID = append(g.lastID, '-')
		g.lastID = strconv.AppendInt(g.lastID, g.n, 10)
	}
	typ := eventTypes[int(g.n)%len(eventTypes)]

	b := g.buf[:0]
	b = append(b, `{"id":"`...)
	b = append(b, g.lastID...)
	b = append(b, `","type":"`...)
	b = append(b, typ...)
	b = append(b, `","channel":"webhook","destination":"`...)
	b = append(b, g.dest...)
	b = append(b, `","payload":`...)
	b = append(b, g.payload...)
	b = append(b, '}')
	g.buf = b
	return b
}

func makePayload(size int) []byte {
	if size < 32 {
		size = 32
	}
	filler := make([]byte, 0, size)
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	for len(filler) < size-30 {
		filler = append(filler, alphabet[len(filler)%len(alphabet)])
	}
	return []byte(fmt.Sprintf(
		`{"valor":1500,"moeda":"BRL","ref":"%s"}`, filler))
}

func newClient(conc int) *http.Client {
	idle := conc * 2
	if idle < 64 {
		idle = 64
	}
	return &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			DialContext:         (&net.Dialer{Timeout: 3 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			MaxIdleConns:        idle * 2,
			MaxIdleConnsPerHost: idle,
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  true,
		},
	}
}

func resetTarget(c *http.Client, target string) error {
	req, err := http.NewRequest(http.MethodPost, target+"/admin/reset", nil)
	if err != nil {
		return err
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.Body.Close()
}

func fetchStats(c *http.Client, target string) (*metrics.Snapshot, error) {
	resp, err := c.Get(target + "/stats")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var s metrics.Snapshot
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return nil, err
	}
	return &s, nil
}

// waitDrain espera a fila esvaziar e nada mais estar em voo.
func waitDrain(c *http.Client, target string, max time.Duration) (*metrics.Snapshot, bool) {
	deadline := time.Now().Add(max)
	var last *metrics.Snapshot
	stable := 0
	for time.Now().Before(deadline) {
		s, err := fetchStats(c, target)
		if err == nil {
			last = s
			if s.QueueLen == 0 && s.InFlight == 0 {
				stable++
				// Duas leituras seguidas zeradas evitam declarar o fim
				// num vale momentaneo entre dois lotes.
				if stable >= 2 {
					return s, true
				}
			} else {
				stable = 0
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	return last, false
}

func writeResult(path string, r bench.Result) error {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

func printSummary(r bench.Result) {
	fmt.Printf("\n===== %s / %s =====\n", r.Label, r.Scenario)
	fmt.Printf("enviados          %d em %.1fs\n", r.Sent, r.DurationSeconds)
	fmt.Printf("tentativa         %.0f req/s\n", r.AttemptedRPS)
	fmt.Printf("aceitos (202)     %.0f req/s\n", r.AcceptedRPS)
	fmt.Printf("status            %v\n", r.StatusCounts)
	if r.TransportErrors > 0 {
		fmt.Printf("erros de rede     %d\n", r.TransportErrors)
	}
	l := r.ClientLatencyMs
	fmt.Printf("latencia API      p50 %.2f  p95 %.2f  p99 %.2f  max %.2f ms\n",
		l.P50Ms, l.P95Ms, l.P99Ms, l.MaxMs)
	fmt.Printf("dreno             %.1fs (completo=%v)\n", r.DrainSeconds, r.DrainComplete)
	fmt.Printf("entregues         %d  (%.0f/s fim a fim)\n", r.DeliveredTotal, r.DeliveryRPS)
	if s := r.Server; s != nil {
		fmt.Printf("servidor          dlq=%d retries=%d duplicados=%d 503=%d\n",
			s.DeadLettered, s.Retries, s.Duplicated, s.RejectedFull)
		fmt.Printf("entrega p99       %.1f ms\n", s.DeliveryLatencyMs.P99Ms)
		fmt.Printf("memoria heap      %.1f MB  goroutines=%d\n",
			float64(s.Runtime.HeapAllocBytes)/1048576, s.Runtime.Goroutines)
	}
}

func rateLabel(r float64) string {
	if r <= 0 {
		return "maxima (malha fechada)"
	}
	return fmt.Sprintf("%.0f req/s (malha aberta)", r)
}

func hostname() string {
	h, _ := os.Hostname()
	return h
}

func logf(quiet bool, format string, a ...any) {
	if !quiet {
		log.Printf(format, a...)
	}
}
