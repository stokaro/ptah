// Package embedbackend is the contract between the inference lifecycle and the
// database a generation lives in.
//
// The lifecycle itself is engine-neutral: the engine reads pages and commits
// batches, the decision packages judge structure, coverage and freshness, and
// the run-state rules are [embedstore]'s. What differs per engine is how the
// run state is stored, how a source is read, how a vector and its checkpoint
// are written in one transaction, and how an index is built and searched.
// Those are what a [Backend] answers, and the native CLI, the readiness gate
// and the agent tools reach every engine through it, so each verb has one
// implementation whatever the database is (stokaro/ptah#4181).
//
// The package holds the contract, the values it passes, and the decisions that
// are the same on every engine. It imports no engine.
package embedbackend

import (
	"context"
	"strings"
	"time"

	"ptah.run/core/renderer"
	"ptah.run/internal/embedcatchup"
	"ptah.run/internal/embedengine"
	"ptah.run/internal/embedeval"
	"ptah.run/internal/embedgen"
	"ptah.run/internal/embedrun"
	"ptah.run/internal/embedstore"
	"ptah.run/internal/embedverify"
)

// Backend is one database's half of an inference migration.
//
// A Backend owns one open connection. Every method that reaches the database
// takes a context, because each is at least one round trip and a verb that
// cannot be canceled holds a lease until it expires.
type Backend interface {
	// Dialect names the engine, as platform.NormalizeDialect spells it.
	Dialect() string
	// Close releases the connection. Closing twice is a no-op.
	Close() error

	// Store is where run state lives.
	Store() Store
	// EnsureSchema creates the run-state tables when they are not there,
	// rendered by service. It is idempotent, because a worker starting is the
	// normal time to call it. The caller selects the renderer, so a backend
	// links no built-in rendering of its own.
	EnsureSchema(ctx context.Context, service renderer.Service) error

	// ResolveRelation answers the physical relation a schema and table spell
	// on this server. A spelling that names no relation is returned as
	// written, so the verb that needs the relation reports its absence.
	ResolveRelation(ctx context.Context, schema, table string) (Relation, error)
	// WithResolvedRelations answers a copy of spec whose source and target
	// name the physical relations. It is for physical identities -- a lock, an
	// outbox, a pointer -- and never for the generation identity.
	WithResolvedRelations(ctx context.Context, spec embedgen.Spec) (embedgen.Spec, error)
	// TargetObjects derives the vector column and index a generation needs on
	// this engine, or says why the engine cannot hold the specification.
	TargetObjects(spec embedgen.Spec) (embedgen.TargetObjects, error)

	// CheckMode refuses a consistency mode this database cannot provide,
	// naming what it lacks. It is asked by prepare and by the plan, so a
	// specification is refused before anything is created or paid for.
	CheckMode(mode embedcatchup.Mode) error
	// PrepareRun creates the target storage, captures the boundary the
	// consistency mode needs, registers the generation and creates the run.
	// Created is false when an idempotent retry found the same run prepared.
	PrepareRun(
		ctx context.Context, spec embedgen.Spec, generation embedstore.Generation,
		run embedrun.Run, mode embedcatchup.Mode,
	) (PrepareResult, error)
	// NewSource reads the specification's source by keyset.
	NewSource(spec embedgen.Spec) (Source, error)
	// NewTarget writes a batch's vectors and the run that checkpoints them in
	// one transaction.
	NewTarget(spec embedgen.Spec) (embedengine.Target, error)
	// Outbox is the change capture on the specification's physical source.
	// An engine that cannot capture changes refuses as CheckMode does.
	Outbox(ctx context.Context, spec embedgen.Spec) (Outbox, error)
	// EnsureRunIndex builds the generation's index and records the run's
	// index phase.
	EnsureRunIndex(ctx context.Context, runID string, spec embedgen.Spec) (IndexOutcome, error)
	// NewSearcher runs evaluation cases against a generation.
	NewSearcher(spec embedgen.Spec) (Searcher, error)

	// TargetColumnExists reports whether the generation's vector column is
	// there yet.
	TargetColumnExists(ctx context.Context, spec embedgen.Spec) (bool, error)
	// ReadStructure reads back the schema verification measures.
	ReadStructure(ctx context.Context, spec embedgen.Spec, activePointer string) (embedverify.Structure, error)
	// VerificationCorpus walks both sides of the comparison in one read.
	VerificationCorpus(ctx context.Context, spec embedgen.Spec) (embedverify.Corpus, error)
	// CountGenerationRows counts the vectors a registered generation holds.
	CountGenerationRows(ctx context.Context, generation embedstore.Generation) (int, error)
	// GenerationIndexExists reports whether a registered generation's index
	// is there.
	GenerationIndexExists(ctx context.Context, generation embedstore.Generation) (bool, error)
	// RetireGeneration destroys one generation and records that it did.
	RetireGeneration(
		ctx context.Context, identity string, expectedPointer embedstore.Pointer,
		expectedRows int, destruction RetirementDestruction,
	) (Retirement, error)

	// Measure answers what a plan reports about the database.
	Measure(ctx context.Context, spec embedgen.Spec) (Measurement, error)
}

// Store is run state with the phase transition every verb records.
type Store interface {
	embedstore.Store
	// ReachPhase records that a run has got as far as a phase. The phase is
	// a high-water mark, so a verb re-run out of order tells the run nothing.
	ReachPhase(ctx context.Context, runID string, to embedrun.Phase) error
}

// Source is a keyset scan and the reread catch-up needs.
type Source interface {
	embedengine.Source
	embedengine.Rereader
}

// Outbox is the change capture catch-up reads and verification measures.
type Outbox interface {
	embedengine.Changes
	// Installed reports whether the capture is in place on the source.
	Installed(ctx context.Context) (bool, error)
	// Unprocessed counts the settled events after a cursor.
	Unprocessed(ctx context.Context, from embedcatchup.Cursor) (int, error)
	// Horizon is the newest position the capture can vouch for.
	Horizon(ctx context.Context) (uint64, error)
	// Prune removes the events every live reader has passed.
	Prune(ctx context.Context) (Pruned, error)
}

// Pruned is what a prune of an outbox found and removed.
type Pruned struct {
	// Found is false when no live reader answered, which is not a license to
	// empty the outbox, and the rest of the value is then empty.
	Found bool
	// Position is the earliest position every usable live feeder has passed,
	// and Holders the positioned runs holding it there.
	Position embedcatchup.Cursor
	Holders  []OutboxFloorHolder
	// Removed counts the events the prune deleted.
	Removed int64
}

// OutboxFloorHolder identifies a run at the earliest position in an outbox.
type OutboxFloorHolder struct {
	// RunID is the identifier an operator passes to `inference abandon`.
	RunID string
	// Generation is the generation that run was building.
	Generation string
}

// Searcher runs an evaluation corpus against a live generation.
type Searcher interface {
	// QueryParameters reports the settings that decide what a search returns,
	// because a recall number taken under other settings is another number.
	QueryParameters(ctx context.Context) (string, error)
	// Search runs one case through the index and without it.
	Search(ctx context.Context, testCase embedeval.Case, vector []float32, depth int) (embedeval.Result, error)
}

// Relation is one physical relation, named the way the server names it.
type Relation struct {
	// Schema is the namespace the relation is in: a PostgreSQL schema, or a
	// YDB directory relative to the database root.
	Schema string
	// Table is the relation's own name.
	Table string
}

// String is the qualified name, for a diagnostic.
func (r Relation) String() string {
	return embedstore.QualifiedName(r.Schema, r.Table)
}

// IndexOutcome is what an index build did, for a caller that has to say so.
type IndexOutcome string

const (
	// IndexNotDeclared means the specification names no index method. It is
	// an answer rather than a failure: an author who wrote no method asked
	// for exhaustive search.
	IndexNotDeclared IndexOutcome = "not declared"
	// IndexAlreadyValid means the index was there and usable.
	IndexAlreadyValid IndexOutcome = "already valid"
	// IndexRebuilt means an index that was not usable was dropped and built
	// again.
	IndexRebuilt IndexOutcome = "rebuilt"
	// IndexBuilt means the index was created.
	IndexBuilt IndexOutcome = "built"
)

// PrepareResult is the durable run a prepare produced. Created is false when
// an idempotent retry found that exact nonterminal run already prepared.
type PrepareResult struct {
	Run     embedrun.Run
	Created bool
}

// RetirementDestruction is what an approved retirement destroys.
//
// One value rather than three booleans in a parameter list, because they are
// one decision: which storage a generation's vectors are in decides whether
// removing them drops columns or a table, and the approval binds to these
// facts through the retirement plan.
type RetirementDestruction struct {
	// IndexExists is the index state the retirement was approved against.
	IndexExists bool
	// DropColumns removes the vector column and its metadata columns from a
	// relation the application keeps.
	DropColumns bool
	// DropTable removes the relation Ptah created for the generation.
	DropTable bool
}

// Retirement is what a retirement did.
type Retirement struct {
	// RetiredAt is the instant recorded in the generation registry.
	RetiredAt time.Time
	// Notes are sentences an operator is told about what else changed, such
	// as the change capture a retirement removed or left.
	Notes []string
}

// Measurement is what a plan reports about the database.
type Measurement struct {
	// TargetColumnExists reports whether the generation's column is there.
	TargetColumnExists bool
	// SourceExists and TargetTableExists report whether the two relations
	// the specification names are there.
	SourceExists      bool
	TargetTableExists bool
	// Rows is the number of source rows in scope, negative when the source
	// was not counted.
	Rows int64
	// Capabilities are what the database can do with vectors, by the names
	// the plan holds a database to. An absent key was not established.
	Capabilities map[string]bool
	// IndexBuildable reports whether the index the specification declares
	// can be built here.
	IndexBuildable bool
	// Refusals are reasons this database cannot run the specification at
	// all, each naming what is missing.
	Refusals []string
}

// HasIndex reports whether a specification declares an index method.
//
// A specification naming none asks for exhaustive search, which every engine
// answers the same way, so the question is asked of the specification rather
// than of any engine's derivation.
func HasIndex(spec embedgen.Spec) bool {
	return strings.TrimSpace(spec.Target.IndexMethod) != ""
}
