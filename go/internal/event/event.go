// Package event define o evento de notificacao e suas regras de validacao.
//
// O evento e o "envelope" que a API recebe. A API valida o envelope
// (id, tipo, canal, destino) mas trata o Payload como opaco: ela nao
// interpreta o conteudo, apenas repassa. Por isso Payload e um
// json.RawMessage - os bytes crus sao guardados sem alocar um mapa
// que ninguem vai ler.
package event

import (
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"strings"
)

// Event e uma notificacao a ser entregue a um sistema externo.
type Event struct {
	// ID e a chave de idempotencia. Vem do CLIENTE, nao do servidor:
	// se a resposta se perder na rede e o cliente reenviar, o mesmo ID
	// chega de novo e permite deduplicar. Um ID gerado pelo servidor
	// seria diferente a cada tentativa e nao serviria para isso.
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	Channel     string          `json:"channel"`
	Destination string          `json:"destination"`
	Payload     json.RawMessage `json:"payload"`
}

// ValidationError agrega TODOS os problemas encontrados de uma vez.
// Reportar so o primeiro obriga quem consome a API a descobrir os
// erros um por um, uma requisicao de cada vez.
type ValidationError struct {
	Problems []string
}

func (e *ValidationError) Error() string {
	return "evento invalido: " + strings.Join(e.Problems, "; ")
}

// DefaultTypes sao os tipos de evento aceitos. Validar contra um
// conjunto fechado (lista branca) evita que texto livre entre no
// sistema e vaze para logs, metricas e roteamento.
func DefaultTypes() []string {
	return []string{
		"payment.approved",
		"payment.refused",
		"payment.refunded",
		"order.created",
		"order.shipped",
		"delivery.updated",
	}
}

// DefaultChannels sao os canais de entrega aceitos.
func DefaultChannels() []string {
	return []string{"webhook"}
}

// Validator guarda as regras de validacao. E uma struct (e nao funcoes
// soltas) para que as regras sejam configuraveis e testaveis sem HTTP.
type Validator struct {
	types    map[string]struct{}
	channels map[string]struct{}

	// AllowPrivateDestination libera destinos em IP privado/loopback.
	// Em producao fica false (protecao contra SSRF). Nos benchmarks
	// fica true, porque o provedor simulado roda em localhost.
	AllowPrivateDestination bool

	// MaxPayloadBytes limita o tamanho do payload ja decodificado.
	MaxPayloadBytes int
}

// NewValidator cria um validador com as listas informadas. Listas
// vazias usam os padroes.
func NewValidator(types, channels []string, allowPrivate bool, maxPayload int) *Validator {
	if len(types) == 0 {
		types = DefaultTypes()
	}
	if len(channels) == 0 {
		channels = DefaultChannels()
	}
	if maxPayload <= 0 {
		maxPayload = 64 * 1024
	}
	v := &Validator{
		types:                   make(map[string]struct{}, len(types)),
		channels:                make(map[string]struct{}, len(channels)),
		AllowPrivateDestination: allowPrivate,
		MaxPayloadBytes:         maxPayload,
	}
	for _, t := range types {
		v.types[t] = struct{}{}
	}
	for _, c := range channels {
		v.channels[c] = struct{}{}
	}
	return v
}

// Validate devolve nil quando o evento esta bem formado.
func (v *Validator) Validate(e Event) error {
	var problems []string

	if strings.TrimSpace(e.ID) == "" {
		problems = append(problems, "campo 'id' e obrigatorio")
	} else if len(e.ID) > 128 {
		problems = append(problems, "campo 'id' excede 128 caracteres")
	}

	if strings.TrimSpace(e.Type) == "" {
		problems = append(problems, "campo 'type' e obrigatorio")
	} else if _, ok := v.types[e.Type]; !ok {
		problems = append(problems, fmt.Sprintf("campo 'type' invalido: %q", e.Type))
	}

	// O canal tem padrao: ausente vira "webhook". Um valor explicito
	// e desconhecido, porem, e erro - assumir um padrao nesse caso
	// entregaria por um canal que o cliente nao pediu.
	if ch := strings.TrimSpace(e.Channel); ch != "" {
		if _, ok := v.channels[ch]; !ok {
			problems = append(problems, fmt.Sprintf("campo 'channel' invalido: %q", ch))
		}
	}

	if strings.TrimSpace(e.Destination) == "" {
		problems = append(problems, "campo 'destination' e obrigatorio")
	} else if err := v.validateDestination(e.Destination); err != nil {
		problems = append(problems, err.Error())
	}

	if len(e.Payload) > v.MaxPayloadBytes {
		problems = append(problems, fmt.Sprintf(
			"campo 'payload' excede %d bytes", v.MaxPayloadBytes))
	}

	if len(problems) > 0 {
		return &ValidationError{Problems: problems}
	}
	return nil
}

// validateDestination protege contra SSRF barato: exige http(s) e
// recusa IPs literais privados.
//
// A protecao completa NAO mora aqui. Resolver DNS nesta funcao custaria
// uma consulta por requisicao no caminho quente, e ainda assim seria
// burlavel por DNS rebinding (o nome resolve para um IP publico na
// validacao e para 127.0.0.1 na hora de conectar). A defesa real esta
// no pacote safedial, que inspeciona o IP no momento de abrir a conexao.
func (v *Validator) validateDestination(dest string) error {
	u, err := url.Parse(dest)
	if err != nil {
		return fmt.Errorf("campo 'destination' nao e uma URL valida")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("campo 'destination' deve usar http ou https")
	}
	host := u.Hostname()
	if host == "" {
		return fmt.Errorf("campo 'destination' sem host")
	}
	if v.AllowPrivateDestination {
		return nil
	}
	if ip := net.ParseIP(host); ip != nil && !IsPublicIP(ip) {
		return fmt.Errorf("campo 'destination' aponta para endereco interno")
	}
	if strings.EqualFold(host, "localhost") {
		return fmt.Errorf("campo 'destination' aponta para endereco interno")
	}
	return nil
}

// IsPublicIP diz se o IP pode ser alvo de uma entrega externa.
// Recusa loopback, link-local, privado, multicast, nao especificado e
// o range de metadados de nuvem (169.254.0.0/16 ja cai em link-local).
func IsPublicIP(ip net.IP) bool {
	if ip == nil {
		return false
	}
	if ip.IsLoopback() || ip.IsUnspecified() ||
		ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsInterfaceLocalMulticast() || ip.IsMulticast() {
		return false
	}
	if ip.IsPrivate() {
		return false
	}
	// 100.64.0.0/10 - CGNAT, usado por redes internas de nuvem.
	if ip4 := ip.To4(); ip4 != nil {
		if ip4[0] == 100 && ip4[1] >= 64 && ip4[1] <= 127 {
			return false
		}
	}
	return true
}

// Normalize aplica os valores padrao apos a validacao.
func (e *Event) Normalize() {
	if strings.TrimSpace(e.Channel) == "" {
		e.Channel = "webhook"
	}
}
