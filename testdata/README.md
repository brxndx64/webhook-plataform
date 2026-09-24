# Fixtures para teste manual

Suba o servidor e o provedor simulado, e dispare com `curl.exe` a partir
da raiz do repositório.

**Evento válido — espera-se `202 Accepted`:**

```powershell
curl.exe -i -X POST http://localhost:8080/notifications -H "Content-Type: application/json" -d "@testdata/evento.json"
```

**JSON malformado (falta a `}` final) — espera-se `400 Bad Request`:**

```powershell
curl.exe -i -X POST http://localhost:8080/notifications -H "Content-Type: application/json" -d "@testdata/evento-quebrado.json"
```

**Campos obrigatórios ausentes — espera-se `400` com a lista de problemas:**

```powershell
curl.exe -i -X POST http://localhost:8080/notifications -H "Content-Type: application/json" -d "{}"
```

**Método errado — espera-se `405 Method Not Allowed`:**

```powershell
curl.exe -i -X GET http://localhost:8080/notifications
```

Trocando a porta para `8081`, os mesmos comandos valem para a
implementação Python — e devem produzir exatamente as mesmas respostas.

> No PowerShell use `curl.exe`, com a extensão. O `curl` sozinho é um
> apelido para `Invoke-WebRequest`, que tem outra sintaxe e lança exceção
> quando a resposta não é 2xx.
