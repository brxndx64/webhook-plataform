package event

import (
	"encoding/json"
	"strings"
	"testing"
)

func valid() Event {
	return Event{
		ID:          "evt_001",
		Type:        "payment.approved",
		Channel:     "webhook",
		Destination: "https://exemplo.com/hooks",
		Payload:     json.RawMessage(`{"valor":1500}`),
	}
}

func TestValidate(t *testing.T) {
	v := NewValidator(nil, nil, false, 1024)

	tests := []struct {
		name    string
		mutate  func(*Event)
		wantErr string // trecho esperado na mensagem; "" = deve passar
	}{
		{"evento completo", func(*Event) {}, ""},
		{"canal ausente usa padrao", func(e *Event) { e.Channel = "" }, ""},
		{"id ausente", func(e *Event) { e.ID = "" }, "'id'"},
		{"id so com espacos", func(e *Event) { e.ID = "   " }, "'id'"},
		{"id gigante", func(e *Event) { e.ID = strings.Repeat("x", 200) }, "128"},
		{"type ausente", func(e *Event) { e.Type = "" }, "'type'"},
		{"type fora da lista branca", func(e *Event) { e.Type = "qualquer.coisa" }, "'type' invalido"},
		{"canal desconhecido", func(e *Event) { e.Channel = "pombo-correio" }, "'channel'"},
		{"destino ausente", func(e *Event) { e.Destination = "" }, "'destination'"},
		{"destino nao http", func(e *Event) { e.Destination = "ftp://exemplo.com" }, "http"},
		{"destino sem host", func(e *Event) { e.Destination = "https://" }, "'destination'"},
		{"payload gigante", func(e *Event) {
			e.Payload = json.RawMessage(`{"a":"` + strings.Repeat("x", 2000) + `"}`)
		}, "excede"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := valid()
			tt.mutate(&e)
			err := v.Validate(e)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("esperava sucesso, veio: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("esperava erro contendo %q, passou", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("erro %q nao contem %q", err.Error(), tt.wantErr)
			}
		})
	}
}

// Reportar todos os problemas de uma vez evita que quem consome a API
// precise de uma requisicao por campo errado.
func TestValidateReportaTodosOsProblemas(t *testing.T) {
	v := NewValidator(nil, nil, false, 1024)
	err := v.Validate(Event{})
	if err == nil {
		t.Fatal("esperava erro")
	}
	var ve *ValidationError
	if !asValidation(err, &ve) {
		t.Fatalf("esperava *ValidationError, veio %T", err)
	}
	if len(ve.Problems) < 3 {
		t.Fatalf("esperava pelo menos 3 problemas (id, type, destination), veio %d: %v",
			len(ve.Problems), ve.Problems)
	}
}

func asValidation(err error, target **ValidationError) bool {
	v, ok := err.(*ValidationError)
	if ok {
		*target = v
	}
	return ok
}

// SSRF: o destino nao pode apontar para dentro da propria rede.
func TestValidateBloqueiaDestinoInterno(t *testing.T) {
	v := NewValidator(nil, nil, false, 1024)
	internos := []string{
		"http://127.0.0.1:8080/hook",
		"http://localhost/hook",
		"http://169.254.169.254/latest/meta-data/", // metadados de nuvem
		"http://10.0.0.5/hook",
		"http://192.168.1.10/hook",
		"http://[::1]/hook",
	}
	for _, d := range internos {
		t.Run(d, func(t *testing.T) {
			e := valid()
			e.Destination = d
			if err := v.Validate(e); err == nil {
				t.Fatalf("destino interno %q foi aceito", d)
			}
		})
	}
}

func TestValidatePermitePrivadoQuandoConfigurado(t *testing.T) {
	v := NewValidator(nil, nil, true, 1024)
	e := valid()
	e.Destination = "http://127.0.0.1:9000/hook"
	if err := v.Validate(e); err != nil {
		t.Fatalf("com allowPrivate deveria aceitar: %v", err)
	}
}

func TestNormalizeAplicaCanalPadrao(t *testing.T) {
	e := valid()
	e.Channel = ""
	e.Normalize()
	if e.Channel != "webhook" {
		t.Fatalf("esperava canal padrao 'webhook', veio %q", e.Channel)
	}
}

// O payload precisa chegar ao worker byte a byte como veio, porque a
// plataforma so repassa - ela nao interpreta.
func TestPayloadPreservadoLiteralmente(t *testing.T) {
	original := `{"valor":1500,"moeda":"BRL","aninhado":{"a":[1,2,3]}}`
	var e Event
	body := `{"id":"1","type":"payment.approved","destination":"https://x.com","payload":` + original + `}`
	if err := json.Unmarshal([]byte(body), &e); err != nil {
		t.Fatal(err)
	}
	if string(e.Payload) != original {
		t.Fatalf("payload alterado:\n  veio: %s\n  esperado: %s", e.Payload, original)
	}
}
