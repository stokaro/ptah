package parser

import (
	"strings"

	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/core/platform/capability"
)

// Option configures parser behavior.
type Option func(*Parser)

// WithDialect selects the SQL dialect grammar for syntax that cannot be
// interpreted correctly by the compatibility parser alone.
func WithDialect(dialect string) Option {
	return func(p *Parser) {
		normalized := platform.NormalizeDialect(dialect)
		if normalized == "" {
			normalized = strings.ToLower(strings.TrimSpace(dialect))
		}
		p.dialect = normalized
	}
}

// WithCapabilities sets the target feature set used by dialect-specific
// parser decisions. The set is cloned so callers can reuse or mutate their
// original value safely.
func WithCapabilities(capabilities capability.Capabilities) Option {
	return func(p *Parser) {
		p.capabilities = capabilities.Clone()
	}
}

// Dialect returns the normalized dialect selected for this parser. An empty
// string means compatibility-oriented best-effort mode.
func (p *Parser) Dialect() string {
	return p.dialect
}

// Capabilities returns an independent copy of the parser capability set.
func (p *Parser) Capabilities() capability.Capabilities {
	return p.capabilities.Clone()
}
