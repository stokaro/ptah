// Package registry contains the planner dialect registry used by the public
// migration/planner package and extension implementations.
package registry

import (
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/core/platform/capability"
	"github.com/stokaro/ptah/migration/schemadiff/types"
)

// Planner defines the interface for database-specific migration planning.
type Planner interface {
	GenerateMigrationAST(diff *types.SchemaDiff, generated *goschema.Database) []ast.Node
	GenerateMigrationASTChecked(diff *types.SchemaDiff, generated *goschema.Database) ([]ast.Node, error)
}

// Options configures planner construction.
type Options struct {
	// Capabilities describes the concrete target server. Nil means the
	// dialect's default capability preset.
	Capabilities capability.Capabilities
	// ConcurrentIndexNames requests PostgreSQL CREATE INDEX CONCURRENTLY for
	// exactly these newly added index names when the target supports it.
	ConcurrentIndexNames []string
}

// CapabilitiesFor returns the configured capability set, falling back to the
// default preset for dialect when no explicit set was provided.
func (o Options) CapabilitiesFor(dialect string) capability.Capabilities {
	if o.Capabilities != nil {
		return o.Capabilities
	}
	return capability.ForDialect(dialect)
}

// Factory creates a planner for a normalized dialect using construction
// options supplied by the caller.
type Factory func(Options) Planner

var (
	mu        sync.RWMutex
	factories = map[string]Factory{}
)

// Register registers a planner factory for dialect.
func Register(dialect string, factory Factory) error {
	normalized := normalizeRegistryDialect(dialect)
	if normalized == "" {
		return fmt.Errorf("planner registry: dialect must not be empty")
	}
	if factory == nil {
		return fmt.Errorf("planner registry: factory for dialect %q must not be nil", normalized)
	}

	mu.Lock()
	defer mu.Unlock()

	if _, exists := factories[normalized]; exists {
		return fmt.Errorf("planner registry: dialect %q is already registered", normalized)
	}
	factories[normalized] = factory
	return nil
}

// MustRegister registers a planner factory and panics if registration fails.
func MustRegister(dialect string, factory Factory) {
	if err := Register(dialect, factory); err != nil {
		panic(err)
	}
}

// Get returns a registered planner for dialect.
func Get(dialect string, opts Options) (Planner, error) {
	normalized := normalizeRegistryDialect(dialect)
	if normalized == "" {
		return nil, fmt.Errorf("unsupported database dialect: %s", dialect)
	}

	mu.RLock()
	factory, exists := factories[normalized]
	mu.RUnlock()
	if !exists {
		return nil, fmt.Errorf("unsupported database dialect: %s", dialect)
	}
	planner := factory(opts)
	if planner == nil {
		return nil, fmt.Errorf("planner registry: factory for dialect %q returned nil", normalized)
	}
	return planner, nil
}

// RegisteredDialects returns the registered dialect names in stable order.
func RegisteredDialects() []string {
	mu.RLock()
	defer mu.RUnlock()

	dialects := make([]string, 0, len(factories))
	for dialect := range factories {
		dialects = append(dialects, dialect)
	}
	slices.Sort(dialects)
	return dialects
}

func normalizeRegistryDialect(dialect string) string {
	normalized := platform.NormalizeDialect(dialect)
	if normalized != "" {
		return normalized
	}
	return strings.ToLower(strings.TrimSpace(dialect))
}
