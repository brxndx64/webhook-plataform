#!/usr/bin/env python3
"""Plataforma de webhooks em Python — equivalente a ../go.

Mesmo contrato HTTP, mesmas regras de negocio, mesmos codigos de status
e mesmo formato de /stats que a implementacao Go. Sem isso a comparacao
de performance nao mediria nada.

A abordagem e asyncio + aiohttp, nao codigo bloqueante: comparar Python
sincrono com Go concorrente seria comparar arquiteturas, nao linguagens.

    python app.py --workers 64 --queue 20000

Dashboard ao vivo em http://localhost:8081/dashboard
"""

from __future__ import annotations

import argparse
import asyncio
import gc
import ipaddress
import json
import math
import os
import platform
import random
import sys
import time
from typing import Any
from urllib.parse import urlparse

import aiohttp
from aiohttp import web

IMPL = "python"

# ---------------------------------------------------------------- histograma

# Os MESMOS baldes geometricos da implementacao Go (50us, fator 1.06,
# 253 baldes). Percentis so sao comparaveis se forem calculados do
# mesmo jeito nos dois lados.
MIN_MICROS = 50.0
FACTOR = 1.06
NUM_BUCKETS = 253
LOG_FACTOR = math.log(FACTOR)


class Histogram:
    """Latencias em microssegundos.

    Sem locks: o asyncio roda tudo num unico thread, entao nao ha
    concorrencia real de memoria. E uma vantagem de simplicidade do
    modelo de evento unico — e tambem a razao de ele nao usar mais de
    um nucleo.
    """

    __slots__ = ("buckets", "count", "sum_us", "max_us")

    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self.buckets = [0] * NUM_BUCKETS
        self.count = 0
        self.sum_us = 0.0
        self.max_us = 0.0

    def observe_micros(self, us: float) -> None:
        if us < 0:
            us = 0.0
        if us < MIN_MICROS:
            i = 0
        else:
            i = int(math.log(us / MIN_MICROS) / LOG_FACTOR) + 1
            if i >= NUM_BUCKETS:
                i = NUM_BUCKETS - 1
            elif i < 0:
                i = 0
        self.buckets[i] += 1
        self.count += 1
        self.sum_us += us
        if us > self.max_us:
            self.max_us = us

    @staticmethod
    def _upper_bound_us(i: int) -> float:
        return MIN_MICROS if i <= 0 else MIN_MICROS * (FACTOR ** i)

    def stats(self) -> dict[str, Any]:
        total = self.count
        if total == 0:
            return {"count": 0, "mean_ms": 0.0, "p50_ms": 0.0,
                    "p95_ms": 0.0, "p99_ms": 0.0, "max_ms": 0.0}

        max_ms = self.max_us / 1000.0

        def q(p: float) -> float:
            target = p * total
            cum = 0
            for i, c in enumerate(self.buckets):
                cum += c
                if cum >= target:
                    return min(self._upper_bound_us(i) / 1000.0, max_ms)
            return max_ms

        return {
            "count": total,
            "mean_ms": self.sum_us / total / 1000.0,
            "p50_ms": q(0.50),
            "p95_ms": q(0.95),
            "p99_ms": q(0.99),
            "max_ms": max_ms,
        }


# ------------------------------------------------------------------ metricas

_COUNTERS = (
    "received", "accepted", "duplicated", "invalid", "rejected_full",
    "method_not_allowed", "delivered", "retries", "dead_lettered", "dropped",
)


def _make_rss_reader():
    """Monta uma vez o leitor de memoria residente do proprio processo.

    Sem dependencia externa (psutil), para que a instalacao do Python
    fique com uma unica dependencia e a comparacao permaneca limpa.
    """
    if sys.platform == "win32":
        import ctypes
        from ctypes import wintypes

        class PMC(ctypes.Structure):
            _fields_ = [
                ("cb", wintypes.DWORD),
                ("PageFaultCount", wintypes.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        try:
            k32 = ctypes.WinDLL("kernel32", use_last_error=True)
            # restype explicito e obrigatorio: sem ele o ctypes assume
            # c_int e trunca o pseudo-handle de 64 bits, e a chamada
            # falha em silencio devolvendo zero.
            k32.GetCurrentProcess.restype = wintypes.HANDLE
            handle = k32.GetCurrentProcess()

            fn = getattr(k32, "K32GetProcessMemoryInfo", None)
            if fn is None:
                fn = ctypes.WinDLL("psapi").GetProcessMemoryInfo
            fn.argtypes = [wintypes.HANDLE, ctypes.POINTER(PMC), wintypes.DWORD]
            fn.restype = wintypes.BOOL

            counters = PMC()
            counters.cb = ctypes.sizeof(PMC)

            def read() -> int:
                if fn(handle, ctypes.byref(counters), counters.cb):
                    return int(counters.WorkingSetSize)
                return 0

            read()  # falha cedo se algo estiver errado
            return read
        except Exception:
            return lambda: 0

    try:
        import resource

        def read() -> int:
            rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            # Linux reporta em KiB, macOS em bytes.
            return int(rss * 1024) if sys.platform.startswith("linux") else int(rss)

        return read
    except Exception:
        return lambda: 0


_rss_bytes = _make_rss_reader()


class Metrics:
    def __init__(self, workers: int) -> None:
        self.workers = workers
        self.api = Histogram()
        self.delivery = Histogram()
        self.in_flight = 0
        self.queue = None      # preenchido pelo main
        self.dedup = None
        self.reset()

    def reset(self) -> None:
        for name in _COUNTERS:
            setattr(self, name, 0)
        self.api.reset()
        self.delivery.reset()
        self.start = time.monotonic()

    def snapshot(self) -> dict[str, Any]:
        gc_stats = gc.get_stats()
        collections = sum(int(s.get("collections", 0)) for s in gc_stats)
        return {
            "impl": IMPL,
            "uptime_seconds": time.monotonic() - self.start,
            **{name: getattr(self, name) for name in _COUNTERS},
            "in_flight": self.in_flight,
            "queue_len": self.queue.qsize() if self.queue is not None else 0,
            "queue_cap": self.queue.maxsize if self.queue is not None else 0,
            "dedup_size": len(self.dedup.seen) if self.dedup is not None else 0,
            "workers": self.workers,
            "api_latency_ms": self.api.stats(),
            "delivery_latency_ms": self.delivery.stats(),
            "runtime": {
                # "goroutines" no Go, tarefas do asyncio aqui: o
                # dashboard e o relatorio usam a mesma chave.
                "goroutines": len(asyncio.all_tasks()),
                "heap_alloc_bytes": _rss_bytes(),
                "heap_objects": len(gc.get_objects()) if os.environ.get("DEEP_STATS") else 0,
                "total_alloc_bytes": 0,
                "mallocs_total": 0,
                "num_gc": collections,
                "gc_pause_total_ms": 0.0,
                "num_cpu": os.cpu_count() or 1,
                # 1 de proposito: um processo Python usa um nucleo por
                # causa do GIL. Escalar exige varios processos.
                "gomaxprocs": 1,
                "version": f"python {platform.python_version()}",
            },
        }

    def prometheus(self) -> str:
        s = self.snapshot()
        out: list[str] = []

        def p(name: str, help_: str, typ: str, v: Any) -> None:
            out.append(f"# HELP {name} {help_}\n# TYPE {name} {typ}\n{name} {v}")

        p("webhook_received_total", "Requisicoes recebidas", "counter", s["received"])
        p("webhook_accepted_total", "Eventos aceitos (202)", "counter", s["accepted"])
        p("webhook_duplicated_total", "Barrados por idempotencia", "counter", s["duplicated"])
        p("webhook_invalid_total", "Rejeitados por validacao", "counter", s["invalid"])
        p("webhook_rejected_full_total", "Rejeitados por fila cheia", "counter", s["rejected_full"])
        p("webhook_delivered_total", "Entregas bem-sucedidas", "counter", s["delivered"])
        p("webhook_retries_total", "Retentativas", "counter", s["retries"])
        p("webhook_dead_lettered_total", "Enviados para a DLQ", "counter", s["dead_lettered"])
        p("webhook_queue_length", "Itens na fila", "gauge", s["queue_len"])
        p("webhook_in_flight", "Entregas em andamento", "gauge", s["in_flight"])
        p("webhook_api_latency_p99_ms", "Latencia p99 da API", "gauge", s["api_latency_ms"]["p99_ms"])
        return "\n".join(out) + "\n"


# --------------------------------------------------------------- idempotencia

class DedupStore:
    """IDs vistos numa janela de tempo.

    Um dict simples basta: o event loop e de thread unica, entao nao ha
    a disputa de lock que obrigou a versao Go a fatiar o mapa em 64
    shards.
    """

    def __init__(self, ttl: float) -> None:
        self.ttl = ttl
        self.seen: dict[str, float] = {}

    def check_and_mark(self, event_id: str) -> bool:
        now = time.monotonic()
        at = self.seen.get(event_id)
        if at is not None and now - at < self.ttl:
            return True
        self.seen[event_id] = now
        return False

    def unmark(self, event_id: str) -> None:
        """Desfaz a marcacao de um ID.

        Necessario quando o evento e marcado mas NAO chega a ser aceito
        (fila cheia -> 503). Sem isto o ID ficaria marcado, e a
        retentativa que o proprio Retry-After pediu seria descartada
        como duplicata: o evento se perderia em silencio.
        """
        self.seen.pop(event_id, None)

    def clear(self) -> None:
        """Esvazia o store. Usado apenas pelo /admin/reset dos benchmarks."""
        self.seen.clear()

    def collect(self) -> None:
        cutoff = time.monotonic() - self.ttl
        for k in [k for k, v in self.seen.items() if v < cutoff]:
            del self.seen[k]

    async def gc_loop(self, every: float = 60.0) -> None:
        while True:
            await asyncio.sleep(every)
            self.collect()


# ---------------------------------------------------------------- rate limit

class RateLimiter:
    """Token bucket por destino, igual ao da versao Go."""

    def __init__(self, rate: float, burst: int) -> None:
        self.rate = rate
        self.burst = float(max(burst, 1))
        self.buckets: dict[str, list[float]] = {}  # chave -> [tokens, last]

    @property
    def enabled(self) -> bool:
        return self.rate > 0

    def _reserve(self, key: str) -> float:
        now = time.monotonic()
        b = self.buckets.get(key)
        if b is None:
            b = [self.burst, now]
            self.buckets[key] = b
        elapsed = now - b[1]
        if elapsed > 0:
            b[0] = min(self.burst, b[0] + elapsed * self.rate)
            b[1] = now
        b[0] -= 1
        return 0.0 if b[0] >= 0 else -b[0] / self.rate

    async def wait(self, key: str) -> None:
        if not self.enabled:
            return
        d = self._reserve(key)
        if d > 0:
            await asyncio.sleep(d)


# ---------------------------------------------------------------- validacao

DEFAULT_TYPES = frozenset({
    "payment.approved", "payment.refused", "payment.refunded",
    "order.created", "order.shipped", "delivery.updated",
})
DEFAULT_CHANNELS = frozenset({"webhook"})


class Validator:
    def __init__(self, allow_private: bool, max_payload: int) -> None:
        self.allow_private = allow_private
        self.max_payload = max_payload

    def validate(self, e: dict[str, Any]) -> list[str]:
        """Devolve TODOS os problemas de uma vez (lista vazia = ok)."""
        problems: list[str] = []

        eid = e.get("id")
        if not isinstance(eid, str) or not eid.strip():
            problems.append("campo 'id' e obrigatorio")
        elif len(eid) > 128:
            problems.append("campo 'id' excede 128 caracteres")

        etype = e.get("type")
        if not isinstance(etype, str) or not etype.strip():
            problems.append("campo 'type' e obrigatorio")
        elif etype not in DEFAULT_TYPES:
            problems.append(f"campo 'type' invalido: {etype!r}")

        channel = e.get("channel")
        if isinstance(channel, str) and channel.strip():
            if channel.strip() not in DEFAULT_CHANNELS:
                problems.append(f"campo 'channel' invalido: {channel!r}")

        dest = e.get("destination")
        if not isinstance(dest, str) or not dest.strip():
            problems.append("campo 'destination' e obrigatorio")
        else:
            err = self._validate_destination(dest)
            if err:
                problems.append(err)

        payload = e.get("payload")
        if payload is not None:
            size = len(json.dumps(payload, separators=(",", ":")))
            if size > self.max_payload:
                problems.append(f"campo 'payload' excede {self.max_payload} bytes")

        return problems

    def _validate_destination(self, dest: str) -> str | None:
        try:
            u = urlparse(dest)
        except Exception:
            return "campo 'destination' nao e uma URL valida"
        if u.scheme not in ("http", "https"):
            return "campo 'destination' deve usar http ou https"
        if not u.hostname:
            return "campo 'destination' sem host"
        if self.allow_private:
            return None
        if u.hostname.lower() == "localhost":
            return "campo 'destination' aponta para endereco interno"
        try:
            ip = ipaddress.ip_address(u.hostname)
        except ValueError:
            return None  # e um nome, nao um IP literal
        if (ip.is_loopback or ip.is_private or ip.is_link_local
                or ip.is_multicast or ip.is_unspecified or ip.is_reserved):
            return "campo 'destination' aponta para endereco interno"
        return None


# ---------------------------------------------------------------- dispatcher

class Dispatcher:
    def __init__(self, cfg: argparse.Namespace, q: asyncio.Queue,
                 m: Metrics, session: aiohttp.ClientSession) -> None:
        self.cfg = cfg
        self.q = q
        self.m = m
        self.session = session
        self.limiter = RateLimiter(cfg.rate, cfg.burst)
        self.dlq: list[dict[str, Any]] = []
        self.tasks: list[asyncio.Task] = []
        self._stopping = False

    def start(self) -> None:
        self.tasks = [asyncio.create_task(self._worker(i))
                      for i in range(self.cfg.workers)]

    async def stop(self, timeout: float) -> None:
        """Encerramento gracioso: drenar a fila, depois cancelar."""
        self._stopping = True
        try:
            await asyncio.wait_for(self.q.join(), timeout=timeout)
        except asyncio.TimeoutError:
            self.m.dropped += self.q.qsize()
        for t in self.tasks:
            t.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)

    async def _worker(self, wid: int) -> None:
        while True:
            item = await self.q.get()
            try:
                self.m.in_flight += 1
                await self._deliver(item)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.m.dead_lettered += 1
            finally:
                self.m.in_flight -= 1
                self.q.task_done()

    async def _deliver(self, item: dict[str, Any]) -> None:
        start = time.monotonic()
        dest = item["destination"]
        key = _destination_key(dest)
        body: bytes = item["body"]
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "webhook-plataform/1.0",
            "X-Event-Id": item["id"],
            "X-Event-Type": item["type"],
        }

        last_status = 0
        last_error = ""
        attempt = 0

        for attempt in range(1, self.cfg.retries + 1):
            if attempt > 1:
                self.m.retries += 1
                await asyncio.sleep(self._backoff(attempt))

            await self.limiter.wait(key)

            headers["X-Delivery-Attempt"] = str(attempt)
            try:
                async with self.session.post(
                        dest, data=body, headers=headers,
                        timeout=aiohttp.ClientTimeout(total=self.cfg.delivery_timeout)) as resp:
                    last_status = resp.status
                    # Ler e descartar o corpo devolve a conexao ao pool.
                    await resp.read()
                    if 200 <= resp.status < 300:
                        self.m.delivered += 1
                        self.m.delivery.observe_micros((time.monotonic() - start) * 1e6)
                        return
                    if not _retriable(resp.status, None):
                        break
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                last_status = 0

        self.m.dead_lettered += 1
        self.dlq.append({
            "event": {k: item[k] for k in ("id", "type", "channel", "destination")},
            "attempts": min(attempt, self.cfg.retries),
            "last_status": last_status,
            "last_error": last_error,
            "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        })
        if len(self.dlq) > self.cfg.dlq:
            del self.dlq[:len(self.dlq) - self.cfg.dlq]

    def _backoff(self, attempt: int) -> float:
        d = self.cfg.backoff * (2 ** (attempt - 2))
        d = min(d, self.cfg.backoff_max)
        return random.random() * d  # full jitter


def _retriable(status: int, err: Exception | None) -> bool:
    if err is not None:
        return True
    if status in (408, 429):
        return True
    return status >= 500


def _destination_key(raw: str) -> str:
    try:
        u = urlparse(raw)
        if u.netloc:
            return f"{u.scheme}://{u.netloc}"
    except Exception:
        pass
    return raw


# ------------------------------------------------------------------ handlers

def json_response(data: Any, status: int = 200) -> web.Response:
    return web.Response(
        status=status,
        body=json.dumps(data, separators=(",", ":")).encode(),
        content_type="application/json",
        charset="utf-8",
    )


class App:
    def __init__(self, cfg, m: Metrics, q: asyncio.Queue,
                 dedup: DedupStore, validator: Validator, disp: Dispatcher):
        self.cfg, self.m, self.q = cfg, m, q
        self.dedup, self.validator, self.disp = dedup, validator, disp

    async def health(self, request: web.Request) -> web.Response:
        if request.method not in ("GET", "HEAD"):
            self.m.method_not_allowed += 1
            return json_response({"error": "metodo nao permitido"}, 405)
        return json_response({"status": "ok"})

    async def notifications(self, request: web.Request) -> web.Response:
        start = time.monotonic()
        try:
            return await self._notifications(request)
        finally:
            self.m.api.observe_micros((time.monotonic() - start) * 1e6)

    async def _notifications(self, request: web.Request) -> web.Response:
        if request.method != "POST":
            self.m.method_not_allowed += 1
            return json_response({"error": "metodo nao permitido"}, 405)

        self.m.received += 1

        # 1. Limite de corpo
        if request.content_length and request.content_length > self.cfg.max_body:
            self.m.invalid += 1
            return json_response({"error": "corpo excede o limite"}, 400)

        raw = await request.content.read(self.cfg.max_body + 1)
        if len(raw) > self.cfg.max_body:
            self.m.invalid += 1
            return json_response({"error": "corpo excede o limite"}, 400)

        # 2. Decodificar
        try:
            e = json.loads(raw)
        except Exception as exc:
            self.m.invalid += 1
            return json_response({"error": f"JSON invalido: {exc}"}, 400)
        if not isinstance(e, dict):
            self.m.invalid += 1
            return json_response({"error": "JSON invalido: esperado um objeto"}, 400)

        # 3. Validar
        problems = self.validator.validate(e)
        if problems:
            self.m.invalid += 1
            return json_response({"error": "evento invalido", "problems": problems}, 400)

        eid = e["id"]

        # 4. Idempotencia — 202, nao erro: para o cliente que perdeu a
        # resposta, o resultado final e o mesmo.
        if self.dedup.check_and_mark(eid):
            self.m.duplicated += 1
            return json_response({"status": "duplicate", "id": eid,
                                  "duplicate": True, "queue_depth": self.q.qsize()}, 202)

        # 5. Enfileirar. put_nowait falha em vez de bloquear: bloquear
        # aqui seguraria o handler e derrubaria a latencia da API.
        item = {
            "id": eid,
            "type": e["type"],
            "channel": (e.get("channel") or "webhook"),
            "destination": e["destination"],
            # Serializar UMA vez aqui, e nao a cada tentativa de entrega.
            "body": json.dumps(e.get("payload") if e.get("payload") is not None else {},
                               separators=(",", ":")).encode(),
        }
        try:
            self.q.put_nowait(item)
        except asyncio.QueueFull:
            # O evento NAO foi aceito: liberar o id para que a
            # retentativa pedida pelo Retry-After possa passar.
            self.dedup.unmark(eid)
            self.m.rejected_full += 1
            return web.Response(
                status=503, headers={"Retry-After": "1"},
                body=json.dumps({"error": "fila cheia, tente novamente"}).encode(),
                content_type="application/json")

        # 6. So agora responder.
        self.m.accepted += 1
        return json_response({"status": "accepted", "id": eid,
                              "duplicate": False, "queue_depth": self.q.qsize()}, 202)

    async def stats(self, request: web.Request) -> web.Response:
        return json_response(self.m.snapshot())

    async def metrics(self, request: web.Request) -> web.Response:
        return web.Response(text=self.m.prometheus(),
                            content_type="text/plain", charset="utf-8")

    async def dlq(self, request: web.Request) -> web.Response:
        return json_response(self.disp.dlq)

    async def reset(self, request: web.Request) -> web.Response:
        if request.method != "POST":
            return json_response({"error": "metodo nao permitido"}, 405)
        self.m.reset()
        # Limpar tambem a memoria de idempotencia: os IDs do
        # aquecimento colidiriam com os da fase medida.
        self.dedup.clear()
        return json_response({"status": "reset"})

    async def dashboard(self, request: web.Request) -> web.Response:
        # Reaproveita o mesmo dashboard da versao Go, para que as duas
        # sejam observadas exatamente do mesmo jeito.
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "..", "go", "internal", "api", "dashboard.html")
        try:
            with open(path, "rb") as fh:
                return web.Response(body=fh.read(), content_type="text/html", charset="utf-8")
        except OSError:
            return web.Response(text="dashboard.html nao encontrado; use /stats",
                                content_type="text/plain")

    async def root(self, request: web.Request) -> web.Response:
        raise web.HTTPFound("/dashboard")


# ---------------------------------------------------------------------- main

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--addr", default="0.0.0.0", help="endereco de escuta")
    p.add_argument("--port", type=int, default=8081, help="porta")
    p.add_argument("--workers", type=int, default=(os.cpu_count() or 1) * 8,
                   help="tarefas de entrega concorrentes")
    p.add_argument("--queue", type=int, default=20000, help="capacidade da fila")
    p.add_argument("--retries", type=int, default=3, help="tentativas por evento")
    p.add_argument("--backoff", type=float, default=0.1, help="backoff inicial (s)")
    p.add_argument("--backoff-max", type=float, default=5.0, help="backoff maximo (s)")
    p.add_argument("--delivery-timeout", type=float, default=5.0, help="timeout por tentativa (s)")
    p.add_argument("--rate", type=float, default=0, help="entregas/s por destino (0 = sem limite)")
    p.add_argument("--burst", type=int, default=50, help="rajada do rate limit")
    p.add_argument("--dedup-ttl", type=float, default=600.0, help="janela de idempotencia (s)")
    p.add_argument("--allow-private", action="store_true",
                   help="permitir destinos internos (use apenas em benchmark)")
    p.add_argument("--dlq", type=int, default=1000, help="capacidade da DLQ")
    p.add_argument("--max-body", type=int, default=1 << 20, help="corpo maximo em bytes")
    p.add_argument("--max-payload", type=int, default=64 * 1024, help="payload maximo em bytes")
    p.add_argument("--shutdown-wait", type=float, default=15.0, help="prazo para drenar")
    return p.parse_args(argv)


class Platform:
    """Tudo o que a aplicacao precisa, montado junto.

    Existir como fabrica (e nao como codigo solto dentro do main) e o
    que permite aos testes levantarem a aplicacao inteira em memoria.
    """

    def __init__(self, cfg: argparse.Namespace) -> None:
        self.cfg = cfg
        self.metrics = Metrics(cfg.workers)
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=cfg.queue)
        self.dedup = DedupStore(cfg.dedup_ttl)
        self.metrics.queue, self.metrics.dedup = self.queue, self.dedup

        # limit_per_host alto e tao critico aqui quanto
        # MaxIdleConnsPerHost no Go: sem isso o custo de reabrir
        # conexao domina a medicao.
        self.connector = aiohttp.TCPConnector(
            limit=max(cfg.workers * 2, 100),
            limit_per_host=max(cfg.workers * 2, 100),
            ttl_dns_cache=300,
            keepalive_timeout=90,
        )
        self.session = aiohttp.ClientSession(connector=self.connector)
        self.dispatcher = Dispatcher(cfg, self.queue, self.metrics, self.session)
        self.validator = Validator(cfg.allow_private, cfg.max_payload)
        self.handlers = App(cfg, self.metrics, self.queue,
                            self.dedup, self.validator, self.dispatcher)
        self.gc_task: asyncio.Task | None = None

        app = web.Application(client_max_size=cfg.max_body + 1024)
        h = self.handlers
        app.router.add_route("*", "/health", h.health)
        app.router.add_route("*", "/notifications", h.notifications)
        app.router.add_route("GET", "/stats", h.stats)
        app.router.add_route("GET", "/metrics", h.metrics)
        app.router.add_route("GET", "/dlq", h.dlq)
        app.router.add_route("*", "/admin/reset", h.reset)
        app.router.add_route("GET", "/dashboard", h.dashboard)
        app.router.add_route("GET", "/", h.root)
        self.app = app

    def start_workers(self) -> None:
        self.dispatcher.start()
        self.gc_task = asyncio.create_task(self.dedup.gc_loop(60.0))

    async def close(self, drain_timeout: float = 5.0) -> None:
        await self.dispatcher.stop(drain_timeout)
        if self.gc_task is not None:
            self.gc_task.cancel()
        await self.session.close()
        await self.connector.close()


def default_config(**overrides: Any) -> argparse.Namespace:
    """Configuracao padrao sem ler a linha de comando (usada nos testes)."""
    cfg = parse_args([])
    for k, v in overrides.items():
        setattr(cfg, k, v)
    return cfg


async def amain() -> None:
    cfg = parse_args()

    platform_ = Platform(cfg)
    platform_.start_workers()
    m, q = platform_.metrics, platform_.queue

    runner = web.AppRunner(platform_.app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, cfg.addr, cfg.port, backlog=2048)
    await site.start()

    print(f"servidor no ar  addr={cfg.addr}:{cfg.port} workers={cfg.workers} "
          f"fila={cfg.queue} retries={cfg.retries} rate_por_destino={cfg.rate}", flush=True)
    print(f"dashboard       http://localhost:{cfg.port}/dashboard", flush=True)

    stop = asyncio.Event()

    def _on_signal(*_: Any) -> None:
        stop.set()

    try:
        loop = asyncio.get_running_loop()
        import signal as _signal
        for sig in (_signal.SIGINT, _signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _on_signal)
            except NotImplementedError:
                _signal.signal(sig, _on_signal)  # Windows
    except Exception:
        pass

    try:
        await stop.wait()
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass

    # Ordem do encerramento gracioso, igual a da versao Go:
    # 1) parar o HTTP (nada novo entra)
    # 2) drenar a fila com prazo
    # 3) fechar as conexoes de saida
    print("encerrando: parando de aceitar novos eventos", flush=True)
    await runner.cleanup()
    await platform_.close(cfg.shutdown_wait)
    if q.qsize():
        print(f"prazo esgotado; {q.qsize()} evento(s) descartado(s)", flush=True)

    s = m.snapshot()
    print(f"resumo  recebidos={s['received']} aceitos={s['accepted']} "
          f"entregues={s['delivered']} dlq={s['dead_lettered']} "
          f"api_p99_ms={s['api_latency_ms']['p99_ms']:.2f}", flush=True)


def main() -> None:
    try:
        asyncio.run(amain())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
