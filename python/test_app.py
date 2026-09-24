"""Testes de paridade com a implementacao Go.

Cada teste aqui tem um irmao em ../go/internal/api/api_test.go. Se os
dois lados nao se comportarem igual, a comparacao de performance mede
implementacoes diferentes e nao vale nada.

    python -m unittest discover -v
"""

from __future__ import annotations

import asyncio
import json
import unittest

from aiohttp.test_utils import TestClient, TestServer

import app as mod


EVENTO_VALIDO = {
    "id": "evt_1",
    "type": "payment.approved",
    "channel": "webhook",
    "destination": "http://localhost:9000/hook",
    "payload": {"valor": 1500},
}


class APITestCase(unittest.IsolatedAsyncioTestCase):
    queue_cap = 10

    async def asyncSetUp(self) -> None:
        cfg = mod.default_config(
            workers=0,            # sem workers: os eventos ficam na fila
            queue=self.queue_cap,
            allow_private=True,
            max_payload=4096,
            max_body=4096,
        )
        self.platform = mod.Platform(cfg)
        self.server = TestServer(self.platform.app)
        self.client = TestClient(self.server)
        await self.client.start_server()

    async def asyncTearDown(self) -> None:
        await self.client.close()
        await self.platform.close(0.1)

    @property
    def m(self) -> mod.Metrics:
        return self.platform.metrics

    @property
    def q(self) -> asyncio.Queue:
        return self.platform.queue

    async def post(self, path: str, body):
        data = body if isinstance(body, (str, bytes)) else json.dumps(body)
        return await self.client.post(path, data=data,
                                      headers={"Content-Type": "application/json"})


class TestHealth(APITestCase):
    async def test_get_ok(self):
        r = await self.client.get("/health")
        self.assertEqual(r.status, 200)
        self.assertIn("application/json", r.headers["Content-Type"])
        self.assertEqual((await r.json())["status"], "ok")

    async def test_metodo_errado(self):
        r = await self.client.post("/health")
        self.assertEqual(r.status, 405)


class TestNotifications(APITestCase):
    async def test_aceita_e_enfileira(self):
        r = await self.post("/notifications", EVENTO_VALIDO)
        self.assertEqual(r.status, 202)

        body = await r.json()
        self.assertEqual(body["status"], "accepted")
        self.assertEqual(body["id"], "evt_1")
        self.assertFalse(body["duplicate"])

        self.assertEqual(self.q.qsize(), 1)
        self.assertEqual(self.m.accepted, 1)

        item = self.q.get_nowait()
        self.assertEqual(item["body"], b'{"valor":1500}')

    async def test_metodo_errado(self):
        for method in ("get", "put", "delete"):
            r = await getattr(self.client, method)("/notifications")
            self.assertEqual(r.status, 405, method)
        self.assertEqual(self.m.method_not_allowed, 3)

    async def test_json_malformado(self):
        r = await self.post("/notifications", '{"id":"1","type":')
        self.assertEqual(r.status, 400)
        self.assertEqual(self.q.qsize(), 0)
        self.assertEqual(self.m.invalid, 1)

    async def test_campos_obrigatorios_reportados_juntos(self):
        r = await self.post("/notifications", {})
        self.assertEqual(r.status, 400)
        problems = (await r.json())["problems"]
        self.assertGreaterEqual(len(problems), 3, problems)

    async def test_tipo_fora_da_lista_branca(self):
        e = dict(EVENTO_VALIDO, type="qualquer.coisa")
        r = await self.post("/notifications", e)
        self.assertEqual(r.status, 400)

    async def test_duplicado_responde_202_mas_nao_enfileira(self):
        r1 = await self.post("/notifications", EVENTO_VALIDO)
        r2 = await self.post("/notifications", EVENTO_VALIDO)

        self.assertEqual(r1.status, 202)
        self.assertEqual(r2.status, 202)
        self.assertTrue((await r2.json())["duplicate"])

        self.assertEqual(self.q.qsize(), 1, "o evento deveria entrar uma unica vez")
        self.assertEqual(self.m.duplicated, 1)

    async def test_canal_ausente_usa_padrao(self):
        e = dict(EVENTO_VALIDO)
        del e["channel"]
        r = await self.post("/notifications", e)
        self.assertEqual(r.status, 202)
        self.assertEqual(self.q.get_nowait()["channel"], "webhook")

    async def test_corpo_grande_demais(self):
        e = dict(EVENTO_VALIDO, payload={"x": "a" * 8000})
        r = await self.post("/notifications", e)
        self.assertEqual(r.status, 400)


class TestBackpressure(APITestCase):
    queue_cap = 1

    async def test_fila_cheia_devolve_503(self):
        await self.post("/notifications", EVENTO_VALIDO)
        r = await self.post("/notifications", dict(EVENTO_VALIDO, id="evt_2"))

        self.assertEqual(r.status, 503)
        self.assertEqual(r.headers.get("Retry-After"), "1")
        self.assertEqual(self.m.rejected_full, 1)


class TestObservabilidade(APITestCase):
    async def test_stats(self):
        await self.post("/notifications", EVENTO_VALIDO)
        r = await self.client.get("/stats")
        self.assertEqual(r.status, 200)

        s = await r.json()
        self.assertEqual(s["impl"], "python")
        self.assertEqual(s["accepted"], 1)
        self.assertEqual(s["queue_cap"], self.queue_cap)
        # Mesmas chaves que o snapshot do Go: o dashboard e o relatorio
        # sao os mesmos para as duas implementacoes.
        for chave in ("received", "duplicated", "invalid", "rejected_full",
                      "delivered", "retries", "dead_lettered", "in_flight",
                      "queue_len", "workers", "api_latency_ms",
                      "delivery_latency_ms", "runtime"):
            self.assertIn(chave, s)
        for chave in ("p50_ms", "p95_ms", "p99_ms", "max_ms", "count", "mean_ms"):
            self.assertIn(chave, s["api_latency_ms"])

    async def test_reset(self):
        await self.post("/notifications", EVENTO_VALIDO)
        r = await self.client.post("/admin/reset")
        self.assertEqual(r.status, 200)
        self.assertEqual(self.m.accepted, 0)

    async def test_prometheus(self):
        r = await self.client.get("/metrics")
        self.assertEqual(r.status, 200)
        self.assertIn("webhook_accepted_total", await r.text())


class TestSSRF(unittest.TestCase):
    """O destination e uma URL que o SERVIDOR vai chamar. Se aceitar
    endereco interno, vira ponte para dentro da infraestrutura."""

    def setUp(self):
        self.v = mod.Validator(allow_private=False, max_payload=4096)

    def test_bloqueia_enderecos_internos(self):
        internos = [
            "http://127.0.0.1:8080/hook",
            "http://localhost/hook",
            "http://169.254.169.254/latest/meta-data/",
            "http://10.0.0.5/hook",
            "http://192.168.1.10/hook",
        ]
        for d in internos:
            with self.subTest(destino=d):
                problems = self.v.validate(dict(EVENTO_VALIDO, destination=d))
                self.assertTrue(problems, f"{d} foi aceito")

    def test_recusa_esquema_nao_http(self):
        problems = self.v.validate(dict(EVENTO_VALIDO, destination="ftp://exemplo.com"))
        self.assertTrue(problems)

    def test_aceita_publico(self):
        problems = self.v.validate(dict(EVENTO_VALIDO, destination="https://exemplo.com/hook"))
        self.assertEqual(problems, [])


class TestDedup(unittest.TestCase):
    def test_primeira_vez_nao_e_duplicata(self):
        s = mod.DedupStore(60.0)
        self.assertFalse(s.check_and_mark("evt_1"))
        self.assertTrue(s.check_and_mark("evt_1"))

    def test_expira_apos_ttl(self):
        import time
        s = mod.DedupStore(0.05)
        s.check_and_mark("evt_1")
        time.sleep(0.08)
        self.assertFalse(s.check_and_mark("evt_1"))


class TestHistograma(unittest.TestCase):
    """Os percentis precisam bater com os da implementacao Go —
    mesmos baldes, mesma conta."""

    def test_percentis_aproximados(self):
        h = mod.Histogram()
        for i in range(1, 101):
            h.observe_micros(i * 1000)
        s = h.stats()

        self.assertEqual(s["count"], 100)
        for nome, esperado in (("p50_ms", 50), ("p95_ms", 95),
                               ("p99_ms", 99), ("mean_ms", 50.5)):
            self.assertLess(abs(s[nome] - esperado) / esperado, 0.10,
                            f"{nome}={s[nome]:.2f}, esperado ~{esperado}")

    def test_p99_revela_a_cauda(self):
        h = mod.Histogram()
        for _ in range(980):
            h.observe_micros(1000)
        for _ in range(20):
            h.observe_micros(2_000_000)
        s = h.stats()

        self.assertLess(s["p50_ms"], 2)
        self.assertGreater(s["p99_ms"], 1000)

    def test_percentil_nunca_passa_do_maximo(self):
        h = mod.Histogram()
        h.observe_micros(1500)
        s = h.stats()
        self.assertLessEqual(s["p99_ms"], s["max_ms"])


class TestRateLimit(unittest.IsolatedAsyncioTestCase):
    async def test_desabilitado_nao_espera(self):
        import time
        l = mod.RateLimiter(0, 1)
        t0 = time.monotonic()
        for _ in range(100):
            await l.wait("d")
        self.assertLess(time.monotonic() - t0, 0.05)

    async def test_destinos_independentes(self):
        import time
        l = mod.RateLimiter(1, 1)
        await l.wait("a")
        t0 = time.monotonic()
        await l.wait("b")
        self.assertLess(time.monotonic() - t0, 0.05)


if __name__ == "__main__":
    unittest.main(verbosity=2)
