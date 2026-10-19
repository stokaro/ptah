package sqlschema

import (
	"sync"
	"sync/atomic"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
)

// readingFacts is what a source dialect's descriptor says about reading a
// schema file: how its names resolve, how its server names an unnamed index,
// CHECK or foreign key, and what a schema is there.
type readingFacts struct {
	name        string
	names       descriptor.Names
	indexNaming descriptor.IndexNaming
	checkNaming descriptor.CheckNaming
	selection   descriptor.SchemaSelection
	// defaultSchema is the schema an unqualified name means offline.
	defaultSchema    string
	constraintNaming descriptor.ConstraintNaming
	foreignKeyNaming descriptor.ForeignKeyNaming
	columns          descriptor.Columns
	statements       descriptor.Statements
}

// readingFactsCache keeps each dialect name's facts once read. A read asks
// for them for every name a statement writes, and a lookup copies the whole
// descriptor. A registered spelling keeps the descriptor it was first given
// for the life of the process, so the cache is safe; a name no descriptor
// carries is not cached, since a later platform.Register may describe it.
var readingFactsCache sync.Map

// factsOf returns what the descriptor of sourcePlatform says about reading a
// schema file, or the zero facts for a dialect no descriptor describes, the
// empty name among them: names as written, and no unnamed index, CHECK or
// foreign key named.
func factsOf(sourcePlatform string) readingFacts {
	if cached, ok := readingFactsCache.Load(sourcePlatform); ok {
		if facts, ok := cached.(readingFacts); ok {
			return facts
		}
	}
	described, found := platform.Lookup(sourcePlatform)
	if !found {
		return readingFacts{}
	}
	facts := readingFacts{
		name:        described.Name,
		names:       described.Names,
		indexNaming: described.IndexNaming,
		checkNaming: described.CheckNaming,
		selection:   described.Schemas.Selection,

		defaultSchema:    described.DefaultSchema,
		constraintNaming: described.ConstraintNaming,
		foreignKeyNaming: described.ForeignKeyNaming,
		columns:          described.Columns,
		statements:       described.Statements,
	}
	readingFactsCache.Store(sourcePlatform, facts)
	return facts
}

// optionOwner is a dialect whose grammar writes table options, and the
// options.
type optionOwner struct {
	dialect string
	options []descriptor.TableOption
}

// registryFacts is what the SQL reader reads across every registered
// dialect, for one registry: the dialects that own table options, and the
// dialect a read with no dialect reads a DO block as.
type registryFacts struct {
	registry      *descriptor.Registry
	optionOwners  []optionOwner
	doBlockReader string
}

// registryFactsCache keeps the facts of the newest registry asked about. A
// registry is immutable and platform.Register replaces it, so the facts of a
// registry stay true for it.
var registryFactsCache atomic.Pointer[registryFacts]

// factsOfRegistry returns the cross-dialect facts of the process registry.
func factsOfRegistry() *registryFacts {
	registry := platform.Dialects()
	if cached := registryFactsCache.Load(); cached != nil && cached.registry == registry {
		return cached
	}
	facts := &registryFacts{registry: registry}
	for _, described := range registry.Dialects() {
		if len(described.TableOptions) > 0 {
			facts.optionOwners = append(facts.optionOwners, optionOwner{dialect: described.Name, options: described.TableOptions})
		}
		if facts.doBlockReader == "" && described.Statements.DoBlocks != "" {
			facts.doBlockReader = described.Name
		}
	}
	registryFactsCache.Store(facts)
	return facts
}
