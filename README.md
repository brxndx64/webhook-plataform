# Plataforma de entrega de webhooks — Go vs Python

Uma plataforma de e-commerce/fintech recebe eventos (`payment.approved`,
`order.shipped`, `delivery.updated`) e precisa entregá-los por webhook a
sistemas externos. Os destinos são lentos, falham e têm limites de taxa.
A plataforma não pode deixar o cliente esperando pela entrega, nem perder
eventos, nem entregar o mesmo evento duas vezes.

Este repositório implementa **a mesma plataforma duas vezes** — em Go e em
Python — e mede as duas sob condições idênticas.

> O objetivo **não** é mostrar que "Go é mais rápido". É medir cenários
> comparáveis e explicar em que condições cada implementação se comporta
> melhor, com números reprodutíveis e com as limitações declaradas.

---

## Arquitetura

```
                                                   ┌──────────────┐
  cliente ──POST /notifications──▶ API HTTP ──────▶│ fila (buffer)│
                    │                              └──────┬───────┘
              202 Accepted                                │
              400 inválido                        ┌───────▼────────┐
              503 fila cheia                      │  worker pool   │
                                                  └───────┬────────┘
                                                          │
                          ┌───────────────────────────────┼──────────────┐
                          │             │                 │              │
                     timeout por    retry com        rate limit      dead-letter
                      tentativa     backoff+jitter   por destino        queue
                          │             │                 │              │
                          └─────────────┴────────┬────────┴──────────────┘
                                                 ▼
                                    sistema externo (webhook)
```

A API responde **`202 Accepted`**, não `200 OK`. `200` significa "terminei";
a API não terminou nada — ela aceitou o evento e vai processá-lo depois.
É esse desacoplamento que permite responder em menos de 1 ms mesmo quando
o destino demora 5 segundos.

---

## Estrutura

```
webhook-plataform/
├── go/                          implementação Go (sem dependências externas)
│   ├── cmd/api/                 o servidor
│   ├── cmd/provider/            provedor simulado (latência/erro configuráveis)
│   ├── cmd/loadgen/             gerador de carga (usado nos DOIS lados)
│   ├── cmd/report/              gera o relatório HTML comparativo
│   └── internal/
│       ├── event/               modelo do evento + validação + anti-SSRF
│       ├── queue/               fila limitada (backpressure)
│       ├── dedup/               idempotência com TTL, mapa fatiado
│       ├── ratelimit/           token bucket por destino
│       ├── dispatcher/          worker pool, retry, timeout, DLQ
│       ├── metrics/             contadores atômicos + histograma
│       ├── safedial/            bloqueio de conexão a rede interna
│       └── api/                 handlers HTTP + dashboard embutido
├── python/                      implementação Python (asyncio + aiohttp)
│   ├── app.py
│   └── test_app.py              testes de paridade com o lado Go
└── bench/
    ├── compare.ps1              orquestra o benchmark e gera o relatório
    ├── results/                 JSON de cada execução
    └── report.html              relatório comparativo (abre no navegador)
```

---

## Como rodar

### Go

```powershell
cd go
go run ./cmd/api -workers 256 -allow-private
```

### Python

```powershell
cd python
python -m venv .venv
.venv\Scripts\python.exe -m pip install -r requirements.txt
.venv\Scripts\python.exe app.py --workers 256 --allow-private
```

> `-allow-private` / `--allow-private` libera destinos em `localhost`.
> Serve para desenvolvimento e benchmark. **Em produção fica desligado** —
> veja "Proteção contra SSRF" abaixo.

### Ver as métricas

Com o servidor no ar, abra o **dashboard ao vivo**:

- Go: <http://localhost:8080/dashboard>
- Python: <http://localhost:8081/dashboard>

Atualiza a cada segundo e mostra vazão, latência p50/p95/p99, profundidade
da fila, memória e contadores. É a mesma página para as duas
implementações, porque as duas expõem `/stats` no mesmo formato.

Também disponíveis: `/stats` (JSON), `/metrics` (formato Prometheus,
para plugar Grafana) e `/dlq` (eventos que falharam em definitivo).

### Provedor simulado

```powershell
cd go
go run ./cmd/provider -addr :9000 -latency 5ms -jitter 2ms -error-rate 0.05
```

Um destino real varia de latência a cada minuto. Aqui a latência, o jitter
e a taxa de erro são parâmetros fixos, então as duas implementações
enfrentam exatamente as mesmas condições.

---

## O contrato HTTP

### `POST /notifications`

```json
{
  "id": "evt_01H8X",
  "type": "payment.approved",
  "channel": "webhook",
  "destination": "https://cliente.com/hooks/pagamentos",
  "payload": { "valor": 1500, "moeda": "BRL" }
}
```

| Situação | Status | Corpo |
|---|---|---|
| Aceito | `202` | `{"status":"accepted","id":"...","queue_depth":12}` |
| Reenvio do mesmo `id` | `202` | `{"status":"duplicate","duplicate":true}` |
| JSON malformado | `400` | `{"error":"JSON invalido: ..."}` |
| Campos faltando | `400` | `{"error":"evento invalido","problems":[...]}` |
| Fila cheia | `503` | `+ Retry-After: 1` |
| Método errado | `405` | `{"error":"metodo nao permitido"}` |

Rotas: `GET /health`, `POST /notifications`, `GET /stats`,
`GET /metrics`, `GET /dlq`, `GET /dashboard`, `POST /admin/reset`.

---

## Decisões de engenharia

As partes que valem ser explicadas numa entrevista.

### O `id` vem do cliente, não do servidor

É o que torna a idempotência possível. Siga o cenário com o servidor
gerando o ID:

```
cliente  →  "cobrar R$ 1500"
servidor →  gera evt_1, aceita, responde 202
            ...a resposta se perde na rede...
cliente  →  não recebeu nada, reenvia
servidor →  gera evt_2 — não tem como saber que é o mesmo
            → CLIENTE COBRADO DUAS VEZES
```

Com o ID nascendo junto com a intenção, do lado do cliente, a repetição
chega com a mesma identidade e é reconhecida. É o padrão
`Idempotency-Key` da Stripe.

Detalhe que só aparece na prática: quando a fila está cheia e a API
devolve `503`, a marcação de idempotência precisa ser **desfeita**.
Sem isso, a retentativa que o próprio `Retry-After` pediu seria
descartada como duplicata e o evento se perderia em silêncio.

### `payload` é `json.RawMessage`, não `map[string]any`

A API valida o **envelope** (id, tipo, canal, destino) mas trata o payload
como opaco — ela só repassa. Decodificar o payload num mapa alocaria uma
estrutura por campo que ninguém vai ler.

`json.RawMessage` guarda os bytes crus: o decodificador valida que é JSON
bem-formado e para por aí. Quem precisar do conteúdo decodifica depois,
para uma struct tipada:

```go
var p PagamentoPayload
json.Unmarshal(evento.Payload, &p)   // int de verdade, sem type assertion
```

O padrão se chama **decodificação em duas fases**. O Python não tem
equivalente direto — `json.loads` decodifica tudo — e essa é uma
diferença estrutural real, não um teste enviesado.

### Fila limitada, e não ilimitada

Uma fila sem limite não resolve sobrecarga: ela só troca "recusar agora"
por "estourar a memória daqui a pouco". Com a fila cheia, a API devolve
`503` com `Retry-After` — **backpressure**: recusar explicitamente é
melhor que aceitar trabalho que não se consegue fazer e degradar para
todo mundo.

O `Enqueue` também nunca bloqueia (`select` com `default`). Bloquear ali
seguraria a goroutine do handler HTTP e derrubaria a latência da API.

### Retry só onde faz sentido

| Resposta do destino | Decisão | Por quê |
|---|---|---|
| erro de rede, timeout | retenta | transitório |
| `429 Too Many Requests` | retenta | "desacelere", não "desista" |
| `5xx` | retenta | problema do outro lado |
| `4xx` (exceto 429) | **desiste** → DLQ | a requisição está errada; repetir dá o mesmo erro |

O backoff é exponencial com **full jitter** — o intervalo é sorteado em
`[0, backoff]`. Sem o jitter, mil eventos que falharam juntos (porque o
destino caiu) voltariam juntos, no mesmo instante, derrubando o destino de
novo assim que ele se levantasse.

O timeout é **por tentativa**, não para o ciclo todo: um destino que
aceita a conexão e nunca responde seguraria um worker para sempre.

### Rate limit por destino, não global

O recurso escasso é o servidor do cliente. Um limite global faria um
destino lento roubar a capacidade de todos os outros. Um cliente que
aguenta 10 req/s não deve receber 1000 só porque a plataforma consegue
produzir.

O saldo do balde pode ficar negativo de propósito, reservando capacidade
futura: sem isso vários workers veriam "zero fichas", dormiriam o mesmo
intervalo e acordariam juntos — uma estampida em vez de um fluxo.

### Proteção contra SSRF

`destination` é uma **URL que o servidor vai chamar**. É a vulnerabilidade
clássica de plataformas de webhook: o atacante cadastra

```json
"destination": "http://169.254.169.254/latest/meta-data/"
```

e o servidor, rodando na nuvem, busca credenciais internas e as entrega.

Validar a URL no cadastro **não basta**: um domínio pode resolver para um
IP público na validação e para `127.0.0.1` na hora de conectar (*DNS
rebinding*). Por isso a checagem definitiva está no hook `Control` do
dialer (`internal/safedial`), que roda **depois** da resolução de nome e
**antes** do connect, sobre o IP que será realmente usado.

Resolver DNS na validação também custaria uma consulta por requisição no
caminho quente.

### Percentis, não média

A média esconde a cauda. Num sistema com 99% das respostas em 1 ms e 1%
em 2 s, a média fica em ~21 ms e parece ótima — enquanto um cliente a cada
cem espera dois segundos. O `p99` revela isso.

O histograma usa 253 baldes geométricos (cada um 6% maior que o anterior),
com memória **constante**. Guardar todas as amostras para ordenar daria
percentil exato, mas alocaria memória proporcional ao número de
requisições — distorcendo justamente a medição que se quer fazer.
As duas implementações usam os mesmos baldes, senão os percentis não
seriam comparáveis.

### Timeouts no servidor

`http.ListenAndServe(addr, nil)` não define timeout nenhum. Um cliente
pode abrir conexão, mandar um byte por minuto e segurar recursos
indefinidamente (ataque *Slowloris*). Daí o `http.Server` com
`ReadHeaderTimeout`, `ReadTimeout`, `WriteTimeout` e `IdleTimeout`
explícitos.

### Pool de conexões de saída

Sem `MaxIdleConnsPerHost` alto, o Go mantém só 2 conexões ociosas por host
e reabre socket a cada entrega — o custo de handshake passa a dominar e o
benchmark mede o pool, não a plataforma. O equivalente no Python é
`TCPConnector(limit_per_host=...)`. E é obrigatório **drenar e fechar** o
corpo da resposta, senão a conexão não volta para o pool.

---

## Benchmark

```powershell
cd bench
.\compare.ps1
```

Parâmetros: `-Duration`, `-Concurrency`, `-Rate`, `-Workers`,
`-ProviderLatency`, `-ProviderErrorRate`, `-Scenario`.

O script compila os binários, sobe o provedor simulado, roda cada
implementação **uma de cada vez** (para uma não roubar CPU da outra),
amostra CPU e memória do processo de fora, e gera `bench/report.html`.

### O que é mantido igual

- mesmo gerador de carga e mesmo provedor simulado para os dois lados;
- mesmos parâmetros: workers, fila, retries, payload, duração, concorrência;
- mesmo aquecimento antes de zerar as métricas — e o aquecimento é drenado
  antes do reset, senão seus eventos contariam na fase medida;
- IDs com prefixo aleatório por execução, senão a janela de idempotência
  trataria a fase medida como repetição do aquecimento;
- CPU e memória medidas **de fora do processo**, na mesma unidade — o
  "heap" que cada runtime reporta internamente não é comparável.

### Malha aberta vs malha fechada

Por padrão o gerador trabalha em **malha aberta**: as chegadas são
agendadas a uma taxa fixa e a latência é medida a partir do **instante
agendado**, não do instante em que a requisição conseguiu sair.

Isso corrige a **omissão coordenada**: um gerador de malha fechada, quando
o servidor trava por 1 s, simplesmente para de enviar e nunca registra
aquela espera — reportando uma latência otimista e falsa.

Com `-Rate 0` o gerador dispara o mais rápido possível. Isso mede a
capacidade de **ingestão** da API, mas satura a fila (a ingestão supera em
muito a entrega) e o teste passa a medir a recusa por backpressure.
É um cenário válido, desde que lido como tal.

---

## Resultados

Execução única em **Windows 11, 8 núcleos**, provedor simulado a 5 ms,
256 workers, 64 conexões. Abra `bench/report.html` para a versão completa.

### Cenário 1 — carga moderada (1200 req/s, malha aberta, 20 s)

Uma taxa que **as duas implementações sustentam**. Aqui se compara
latência e custo, não capacidade.

| Métrica | Go | Python |
|---|---:|---:|
| aceitos | 1.200/s | 1.200/s |
| entregues fim a fim | 1.147/s | 1.149/s |
| API p50 | **0,43 ms** | 4,99 ms |
| API p95 | **0,82 ms** | 27,04 ms |
| API p99 | **2,48 ms** | 43,10 ms |
| entrega p99 | **8,0 ms** | 48,4 ms |
| CPU | **21,3 s** | 23,6 s |
| RSS de pico | 95,9 MB | **60,9 MB** |

Mesma vazão, porque a carga foi imposta de fora e ambas dão conta.
A diferença aparece na **latência: cerca de 10× na cauda**. O Python
gasta menos memória residente.

### Cenário 2 — ingestão máxima (malha fechada, 15 s)

O gerador dispara o mais rápido que consegue. Aqui se mede o **teto**.

| Métrica | Go | Python |
|---|---:|---:|
| aceitos | **20.406/s** | 1.197/s |
| entregues fim a fim | **19.348/s** | 1.134/s |
| total entregue em 15 s | **300.052** | 17.668 |
| API p50 | **2,21 ms** | 51,33 ms |
| API p99 | **17,98 ms** | 86,72 ms |
| CPU | 57,0 s | **19,8 s** |
| RSS de pico | 151,5 MB | **62,8 MB** |
| **entregues por segundo de CPU** | **5.269** | 894 |

**17× mais vazão** — e, mais revelador, **5,9× mais trabalho por segundo
de CPU**. A diferença não é só "o Go usa os 8 núcleos e o Python usa 1":
mesmo normalizando por CPU consumida, o Go entrega quase seis vezes mais
por ciclo gasto.

Repare também no consumo absoluto: o Python usou **menos** CPU total
(19,8 s contra 57,0 s) — porque estava limitado a um núcleo pelo GIL e
simplesmente não conseguiu trabalhar mais. Consumo baixo de CPU aqui não
é eficiência, é teto.

### Leitura honesta

- Onde a carga **cabe** no que o Python aguenta, as duas entregam a mesma
  vazão. A escolha então é entre latência de cauda (Go) e memória
  residente (Python).
- Onde a carga **excede** esse ponto, não há empate: o Python satura em
  ~1.200 req/s por processo e a latência dispara.
- O processo Python é limitado a um núcleo pelo GIL. Em produção
  escalaria com vários processos (um por núcleo) — o que mudaria a foto
  de vazão, mas multiplicaria também a memória e exigiria mover a
  deduplicação e a fila para fora do processo.
- Este cenário é **I/O-bound**, o terreno mais favorável ao Python
  assíncrono. Ainda assim a diferença de eficiência por CPU é grande.

### Limitações

- **Uma execução não é resultado.** Rode 3 a 5 vezes e observe a variação.
- Windows + OneDrive + antivírus introduzem ruído. Um servidor Linux
  ocioso dá números bem mais estáveis.
- Este cenário é **I/O-bound** — o trabalho real é esperar a rede. É onde
  o Python assíncrono compete melhor. Cenários CPU-bound teriam outro
  desfecho.
- O gargalo pode ser o gerador de carga ou o provedor simulado, não o
  servidor. Se os dois lados baterem no mesmo teto, é sinal disso.
- A deduplicação é em memória: não sobrevive a restart nem é compartilhada
  entre instâncias. Em produção seria Redis.

---

## Testes

```powershell
cd go
go test ./...
go vet ./...
```

```powershell
cd python
.venv\Scripts\python.exe -m unittest discover -v
```

Os testes Go chamam os handlers **direto, em memória** (`net/http/httptest`):
sem abrir porta, sem rede, sem servidor. Por isso rodam em milissegundos —
e por isso os handlers são funções nomeadas com dependências injetadas, e
não funções anônimas dentro do `main`.

Cada teste do lado Python tem um irmão no lado Go. Se os dois não se
comportarem igual, a comparação de performance mede implementações
diferentes e não vale nada.

**Detector de corrida:** `go test -race ./...` exige um compilador C no
Windows. Instale o [TDM-GCC](https://jmeubank.github.io/tdm-gcc/) ou o
MinGW-w64 e rode com `$env:CGO_ENABLED=1`.

---

## Próximos passos

- [ ] Persistir a fila (Redis/Postgres) — hoje um restart perde o que está em memória
- [ ] Deduplicação distribuída em Redis
- [ ] Assinatura HMAC das entregas, para o destino verificar a origem
- [ ] Circuit breaker por destino, além do rate limit
- [ ] Reprocessamento da DLQ
- [ ] Rodar o benchmark em Linux, com várias repetições e intervalo de confiança
- [ ] Cenário CPU-bound para contrastar com o I/O-bound atual
- [ ] Python com múltiplos processos, para medir o teto real da linguagem
