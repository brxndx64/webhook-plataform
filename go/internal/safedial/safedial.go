// Package safedial bloqueia conexoes para enderecos internos.
//
// Esta e a defesa real contra SSRF (Server-Side Request Forgery), o
// ataque classico de plataformas de webhook: o atacante cadastra um
// destination apontando para dentro da sua infraestrutura e usa o seu
// proprio servidor como ponte. O alvo classico e o servico de metadados
// de nuvem em 169.254.169.254, que devolve credenciais.
//
// Validar a URL no momento do cadastro NAO basta: um dominio pode
// resolver para um IP publico na validacao e para 127.0.0.1 na hora de
// conectar (DNS rebinding). Por isso a checagem acontece aqui, no hook
// Control do dialer, que roda DEPOIS da resolucao de nome e ANTES do
// connect - exatamente sobre o IP que sera usado.
package safedial

import (
	"fmt"
	"net"
	"syscall"

	"github.com/brxndx64/webhook-plataform/go/internal/event"
)

// Control devolve a funcao de controle para net.Dialer.
// allowPrivate=true desliga a protecao (necessario nos benchmarks,
// em que o provedor simulado roda em localhost).
func Control(allowPrivate bool) func(network, address string, c syscall.RawConn) error {
	if allowPrivate {
		return nil
	}
	return func(network, address string, _ syscall.RawConn) error {
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return fmt.Errorf("endereco invalido %q: %w", address, err)
		}
		ip := net.ParseIP(host)
		if ip == nil {
			return fmt.Errorf("endereco nao resolvido para IP: %q", host)
		}
		if !event.IsPublicIP(ip) {
			return fmt.Errorf("conexao bloqueada para endereco interno %s", ip)
		}
		return nil
	}
}
