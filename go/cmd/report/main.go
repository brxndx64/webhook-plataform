// Comando report: junta os resultados dos benchmarks num relatorio HTML.
//
//	go run ./cmd/report -in ../bench/results -out ../bench/report.html
//
// O HTML gerado e autocontido (os dados vao embutidos), entao pode ser
// aberto direto do disco ou commitado no repositorio.
package main

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/brxndx64/webhook-plataform/go/internal/bench"
)

//go:embed report.html
var tmpl string

func main() {
	in := flag.String("in", "../bench/results", "diretorio com os arquivos .json")
	out := flag.String("out", "../bench/report.html", "arquivo HTML de saida")
	flag.Parse()

	entries, err := os.ReadDir(*in)
	if err != nil {
		log.Fatalf("nao foi possivel ler %s: %v", *in, err)
	}

	var results []bench.Result
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(strings.ToLower(e.Name()), ".json") {
			continue
		}
		p := filepath.Join(*in, e.Name())
		b, err := os.ReadFile(p)
		if err != nil {
			log.Printf("aviso: %s: %v", e.Name(), err)
			continue
		}
		// Alguns editores e o PowerShell gravam UTF-8 com BOM, que
		// nao e JSON valido.
		b = bytes.TrimPrefix(b, []byte{0xEF, 0xBB, 0xBF})

		var r bench.Result
		if err := json.Unmarshal(b, &r); err != nil {
			log.Printf("aviso: %s nao e um resultado valido: %v", e.Name(), err)
			continue
		}
		results = append(results, r)
	}

	if len(results) == 0 {
		log.Fatalf("nenhum resultado encontrado em %s", *in)
	}

	// Ordem estavel: cenario, depois rotulo.
	sort.SliceStable(results, func(i, j int) bool {
		if results[i].Scenario != results[j].Scenario {
			return results[i].Scenario < results[j].Scenario
		}
		return results[i].Label < results[j].Label
	})

	data, err := json.Marshal(results)
	if err != nil {
		log.Fatal(err)
	}

	// Escapar </script> impede que um dado quebre o HTML.
	data = bytes.ReplaceAll(data, []byte("</"), []byte(`<\/`))

	html := strings.Replace(tmpl, "/*__DATA__*/null", string(data), 1)

	if dir := filepath.Dir(*out); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Fatal(err)
		}
	}
	if err := os.WriteFile(*out, []byte(html), 0o644); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("relatorio com %d resultado(s) gerado em %s\n", len(results), *out)
	for _, r := range results {
		fmt.Printf("  %-10s %-14s  %7.0f req/s aceitos  p99 %6.2f ms  entrega %6.0f/s\n",
			r.Scenario, r.Label, r.AcceptedRPS, r.ClientLatencyMs.P99Ms, r.DeliveryRPS)
	}
}
