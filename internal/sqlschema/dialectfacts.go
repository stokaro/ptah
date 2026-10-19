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
	// foreignKeyNameRules and identifierLimit bound the names the server
	// keeps for an unnamed foreign key and an unnamed CHECK.
	foreignKeyNameRules descriptor.ForeignKeyNameRules
	identifierLimit     descriptor.IdentifierLimit
	columns             descriptor.Columns
	statements          descriptor.Statements
	// routines is the routine grammar the dialect writes its routines in,
	// which says how a routine node's header converts.
	routines string
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

		defaultSchema:       described.DefaultSchema,
		constraintNaming:    described.ConstraintNaming,
		foreignKeyNaming:    described.ForeignKeyNaming,
		foreignKeyNameRules: described.ForeignKeyNameRules,
		identifierLimit:     described.IdentifierLimit,
		columns:             described.Columns,
		statements:          described.Statements,
		routines:            described.Grammar.Routines.Body,
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
// dialect, for one registry: the dialects that own table options and the
// options they own, the dialects that read DO blocks, and the dialect a read
// with no dialect reads a DO block as.
type registryFacts struct {
	registry     *descriptor.Registry
	optionOwners []optionOwner
	// ownedOptions are the options, as the parser reports them, that some
	// dialect's TableOptions state. They are that dialect's alone, so none is
	// kept among the properties of the dialect a table is read as.
	ownedOptions map[string]bool
	// doBlockReaders are the dialects whose DO-block language has a reader,
	// in registry order.
	doBlockReaders []string
	doBlockReader  string
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
	facts := &registryFacts{registry: registry, ownedOptions: make(map[string]bool)}
	for _, described := range registry.Dialects() {
		if len(described.TableOptions) > 0 {
			facts.optionOwners = append(facts.optionOwners, optionOwner{dialect: described.Name, options: described.TableOptions})
		}
		for _, option := range described.TableOptions {
			facts.ownedOptions[option.Option] = true
		}
		if doBlockServices(described.Statements.DoBlocks).Roles != nil {
			facts.doBlockReaders = append(facts.doBlockReaders, described.Name)
		}
	}
	if len(facts.doBlockReaders) > 0 {
		facts.doBlockReader = facts.doBlockReaders[0]
	}
	registryFactsCache.Store(facts)
	return facts
}
