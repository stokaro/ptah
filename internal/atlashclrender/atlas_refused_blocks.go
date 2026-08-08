package atlashclrender

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"

	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
)

// Top-level HCL block spellings an inspected render can emit. Only the ones a
// refusal list or a diagnostic names are constants: the token has to be spelled
// identically by the renderer, by [atlasRefusedBlockTypes], and by the
// conformance run that re-measures that list against the binary, and three
// string literals drift where one constant cannot.
const (
	blockExtension = "extension"
	blockPolicy    = "policy"
	blockSequence  = "sequence"
)

// KeepAtlasRefusedBlocksEnvVar restores the full set of blocks Ptah models on
// the Atlas-compatible surface, including the ones the pinned Atlas community
// binary v1.3.0 refuses.
//
// It is an environment variable and not a flag on purpose. The conformance
// cli-surface tier asserts that `ptah-compat` registers exactly the flags the
// pinned binary registers, so a flag that binary does not have would break the
// very promise this surface exists to keep. Precedent and spelling:
// [go.5x5.cz/ptah/internal/atlassource.AllowExternalSchemaEnvVar].
//
// The variable exists because compatibility must not delete a capability
// (AGENTS.md, "Compatibility never removes a capability"). Ptah models
// extensions, sequences and row-level security policies; defaulting to what the
// community binary can read is a drop-in requirement, but a user porting a
// pipeline that needs those blocks has to be able to get them back on this same
// surface rather than being told to rewrite against native `ptah`.
const KeepAtlasRefusedBlocksEnvVar = "PTAH_ATLAS_INSPECT_ALL_BLOCKS"

// KeepAtlasRefusedBlocks reports whether the opt-in variable is set to a true
// boolean value. Unset, empty, false and unparsable values all keep the
// default, mirroring how [go.5x5.cz/ptah/internal/atlassource] reads its own
// opt-in.
func KeepAtlasRefusedBlocks() bool {
	keep, err := strconv.ParseBool(os.Getenv(KeepAtlasRefusedBlocksEnvVar))
	return err == nil && keep
}

// atlasRefusedBlockTypes lists, per dialect, the top-level block types the
// pinned Atlas community binary v1.3.0 refuses AS A FEATURE: not because of how
// Ptah spells an attribute inside them, but because that build does not model
// the construct at all. One such block anywhere in a file costs the WHOLE file,
// so the Atlas-compatible surface omits them by default rather than emitting a
// document its counterpart cannot read (stokaro/ptah#1251).
//
// Every row is measured, and the message is what makes the row a row. Measured
// on PostgreSQL 17 by starting from a file that binary accepts -- its own
// inspect output for the same database, verified at exit 0 -- and adding one
// block type at a time:
//
//	extension "pgcrypto" {}     exit 1  postgres: extensions are not supported by this version
//	sequence "order_seq" {}     exit 1  postgres: sequences are not supported by this version
//	policy "accounts_all" {}    exit 1  postgres: policies are not supported by this version
//
// The blocks are empty on purpose. A `sequence` carrying the `type = bigint`
// Ptah wrote before #1255 is refused with `There is no variable named "bigint"`
// instead, which is Ptah's own rendering defect and was fixed rather than
// suppressed. Stripping the block to its label separates the two verdicts: what
// survives is a refusal of the block type itself, which no spelling can lift.
//
// Nothing else Ptah's inspect emits belongs here, and that too is measured. Off
// the same accepted base, one block at a time:
//
//	role, function, view, materialized, trigger, permission, range   exit 0
//	wibble "x" {}                                                    exit 0
//
// The last row is why the others are not evidence of support: that binary drops
// a top-level block whose name it does not model and carries on, so exit 0 says
// only "harmless to the file", which is all the compatibility surface needs.
// Suppressing those would lose description for nothing.
//
// The map is keyed by dialect because the refusal is, and that is measured too:
// on SQLite the same three blocks are accepted and dropped exactly like
// `wibble`, all at exit 0. A dialect absent here suppresses nothing.
//
// A later build may model any of these, at which point suppressing it would
// silently withhold something the reader could have used.
// TestOracleAtlasRefusedBlockTypesMatchTheBinary re-measures the list in both
// directions so that turns the Atlas CE Oracle job red rather than going
// unnoticed.
var atlasRefusedBlockTypes = map[string][]string{
	platform.Postgres: {blockExtension, blockPolicy, blockSequence},
}

// atlasRefusesBlock reports whether the pinned binary refuses a whole file for
// containing this block type on this dialect.
func atlasRefusesBlock(dialect, block string) bool {
	return slices.Contains(atlasRefusedBlockTypes[dialect], block)
}

// blockCoverageKinds maps a suppressible block type to the coverage kind the
// comparator consults for it. One map rather than a switch at each use site,
// because a block type this render can omit and no coverage kind records is a
// silent hole of exactly the kind stokaro/ptah#1276 is about.
var blockCoverageKinds = map[string]coverage.Kind{
	blockExtension: coverage.Extension,
	blockPolicy:    coverage.Policy,
	blockSequence:  coverage.Sequence,
}

// notDescribed is what a document rendered by this render does not claim to
// describe.
//
// The claim is about the RULE this surface applies, not about what it happened
// to find. [renderer.omitRefusedBlock] writes an extension block only when
// another block in the document depends on the extension, so for any extension
// name absent from the document a reader genuinely cannot tell "the database
// does not have it" from "nothing named it". That is what a whole-kind record
// says, and it is the only truthful thing to say.
//
// Recording only the objects this particular render omitted would be an
// UNDER-claim: it would assert that the absence of every other extension is
// authoritative, which is false for the same document read against any other
// database. Under-claiming is the destructive direction -- it is how a
// presentation decision becomes `DROP EXTENSION` -- so the record is made
// unconditionally whenever the surface is in omit mode, including for a
// database that has none of these objects at all.
//
// Setting [KeepAtlasRefusedBlocksEnvVar] turns omit mode off, and then the
// document describes everything Ptah models and claims so by carrying no
// record.
func (r *renderer) notDescribed() coverage.Set {
	if !r.omitAtlasRefusedBlocks {
		return coverage.Set{}
	}
	var set coverage.Set
	for _, block := range atlasRefusedBlockTypes[r.dialect] {
		kind, known := blockCoverageKinds[block]
		if !known {
			continue
		}
		set = set.WithKind(kind)
	}
	return set
}

// renderCoverageHeader writes the document's coverage record as directive
// comments below the generated-code marker.
//
// It goes in the document rather than only on the diagnostics stream because
// the command that reads the document back is a different process: `schema
// inspect > file` and `schema apply --to file://file` do not share memory, and
// a warning on a terminal cannot reach the second one.
func (r *renderer) renderCoverageHeader() {
	for _, directive := range r.notDescribed().Directives() {
		r.builder.WriteString("// " + directive + "\n")
	}
}

// omitRefusedBlock decides one object's fate on the Atlas-compatible surface,
// records the decision on the render's diagnostics, and reports whether the
// block is left out.
//
// Suppression is REFERENCE-AWARE rather than a fixed list of block types, and
// the measurement that forces this is the pinned binary's own output. For
//
//	CREATE SEQUENCE order_seq;
//	CREATE TABLE orders (id integer NOT NULL DEFAULT nextval('order_seq'::regclass));
//
// that binary emits `default = sql("nextval('order_seq'::regclass)")` and no
// `sequence` block, because it does not model sequences -- and then cannot read
// its own output back: `pq: relation "order_seq" does not exist`, exit 1,
// measured on PostgreSQL 17. So for that shape there is no faithful output that
// binary can read, including its own; suppression cannot produce one, it can
// only choose which kind of broken.
//
// Copying that hole would be reproducing a defect, which the compatibility
// policy's second half forbids, and it would cost something real: before the
// suppression existed, Ptah read this document back at exit 0. Dropping the
// referencing attribute along with the block would instead describe a database
// that does not exist. So the block stays when the document names it, and the
// diagnostic says so.
//
// The consequence is stated rather than hidden: a document that keeps a
// referenced sequence is not readable by the pinned binary. It is readable by
// Ptah, and it describes the database truthfully, which the two alternatives
// cannot both do.
//
// The question asked of the document is "does anything here still DEPEND on
// this object", which is why the caller passes every name that would stop
// resolving without the block rather than only the block's label. For a
// sequence and a policy those are the same word, so this is all the question
// there is; an extension is decided by [renderer.omitRefusedExtension], where
// the names are usually disjoint from the label and one part of the answer is
// not a name at all.
func (r *renderer) omitRefusedBlock(path, block string, names ...string) bool {
	return r.omitRefused(path, block, func() blockDependency {
		if !r.documentNamesAny(names) {
			return blockDependency{}
		}
		return blockDependency{
			because: "another object in this document depends on it",
			cost:    "leave a reference to an object nothing declares",
		}
	})
}

// omitRefusedExtension decides one extension's fate. It asks
// [renderer.omitRefusedBlock]'s question about two sets of names, and then one
// question those names may not be able to answer.
//
// The names are two sets because an extension is almost never referenced by its
// own label: `isn` supplies the type `isbn`, `pgcrypto` supplies the function
// `gen_salt`. Matching the label alone omitted the extension and left the column
// that needs it behind -- measured on PostgreSQL 17.10 against
//
//	CREATE EXTENSION isn;
//	CREATE TABLE books (id integer PRIMARY KEY, code isbn NOT NULL);
//
// where neither Ptah nor the pinned binary could then read the result back:
// `type "isbn" does not exist`, exit 1 from both, which is not a compatibility
// win because that binary rejects the document too (stokaro/ptah#1266).
//
// An extension can also be needed by a document that spells nothing it supplies.
// PostgreSQL prints an operator class only when it is not the default for the
// key's type on the index's access method, so with btree_gin installed
//
//	CREATE INDEX t_gin ON t USING gin (n int4_ops);   -- n is integer
//
// is stored, and rendered, as `USING gin (n)`. Neither `btree_gin` nor
// `int4_ops` appears anywhere in the document, the name scan finds nothing, and
// the block was dropped at exit 0 -- after which the pinned Atlas community
// binary v1.3.0 refused the result with `create index "t_gin" to table: "t":
// pq: data type integer has no default operator class for access method "gin"`,
// and Ptah's own apply failed identically. Measured on PostgreSQL 17.10
// (stokaro/ptah#1286).
//
// The evidence therefore comes from the reader, which resolved the index's
// operator classes and access method against pg_depend; see
// [goschema.Index.RequiresExtensions]. Asking the document instead -- treating
// `USING gin` as a reference to btree_gin -- is the wrong answer to the same
// question: `gin` is a core access method, and tsvector, jsonb and array columns
// have core GIN operator classes, so that rule would pin btree_gin to indexes
// that do not need it and cost every such document its readability.
//
// The NAME question is asked first, and the ordering is what keeps each
// diagnostic true rather than a preference between them. PostgreSQL prints an
// operator class exactly when it is NOT the default for the key's type on the
// access method, so a printed class is a name the document does carry:
// `CREATE INDEX w_trgm ON w USING gin (txt gin_trgm_ops)` renders
// `ops = "gin_trgm_ops"` and its exclusion-constraint form renders
// `elements = "txt gist_trgm_ops WITH ="`. Both are catalog edges too, so asking
// the catalog first answered "kept because of something the document does not
// spell" about a class sitting in the document two lines above -- measured on
// PostgreSQL 17.10 with pg_trgm installed (stokaro/ptah#1286). Printed classes
// are exactly the non-default ones, and over the 45 extensions in the
// postgres:17 image all 5 of those have names pg_catalog does not also supply,
// so each survives the shadowed-name filter into
// [goschema.Extension.Provides] and the name scan can answer for it:
// citext_pattern_ops, gin__int_ops, gist__intbig_ops, gin_trgm_ops,
// gist_trgm_ops.
func (r *renderer) omitRefusedExtension(path string, extension goschema.Extension) bool {
	return r.omitRefused(path, blockExtension, func() blockDependency {
		// The extension's own name AND everything it supplies: a document that
		// depends on `isn` says `isbn`, never `isn`. Provides is empty for
		// sources with no catalog behind them, and the check then degenerates to
		// the label, which is the most that can be known about such a source.
		if r.documentNamesAny(append([]string{extension.Name}, extension.Provides...)) {
			return blockDependency{
				because: "another object in this document depends on it",
				cost:    "leave a reference to an object nothing declares",
			}
		}
		if r.documentRequiresExtension(extension.Name) {
			return blockDependency{
				because: "the catalog resolved an index or constraint in this document to an operator class" +
					" or access method it supplies",
				cost: "leave an index no database could build",
			}
		}
		return blockDependency{}
	})
}

// blockDependency is why a refused block has to stay and what leaving it out
// would cost the document. A zero value means nothing depends on the block.
//
// Both halves are reported because they are what an operator needs to act on. A
// dependency the document names can be found by searching the document; one that
// exists only because the catalog resolved an operator class cannot be, so a
// diagnostic that only said "something depends on it" would send its reader
// looking for a word that may not be there. Which of the two an extension gets
// is decided by asking the name question first, in [renderer.omitRefusedExtension]
// -- a keep the reader CAN look up must never be reported as one they cannot.
// Neither wording claims anything about the other's evidence: an operator class
// can be both printed in the document and resolved from the catalog.
type blockDependency struct {
	because string
	cost    string
}

// omitRefused reports one block's fate on the diagnostics channel and returns
// whether it is left out. The dependency is a function so that the scan behind
// it never runs on the native surface, which omits nothing.
func (r *renderer) omitRefused(path, block string, dependency func() blockDependency) bool {
	if !r.omitAtlasRefusedBlocks || !atlasRefusesBlock(r.dialect, block) {
		return false
	}
	if depends := dependency(); depends.because != "" {
		r.warn(path, fmt.Sprintf(
			"kept in Atlas-compatible schema inspect output because %s:"+
				" the Atlas community CLI refuses a %s schema file that declares any %s block, so it cannot read"+
				" this document, but omitting the block would %s",
			depends.because, r.dialect, block, depends.cost,
		))
		return false
	}
	r.warn(path, fmt.Sprintf(
		"omitted from Atlas-compatible schema inspect output: the Atlas community CLI refuses a %s schema file"+
			" that declares any %s block, and one of them makes the whole document unreadable to it;"+
			" set %s=1 to keep every block Ptah models",
		r.dialect, block, KeepAtlasRefusedBlocksEnvVar,
	))
	return true
}

// documentRequiresExtension reports whether an object the surviving document
// declares cannot be built without this extension, as the catalog resolved it
// rather than as the document spells it.
//
// This is the half [documentNamesAny] cannot always answer. It reads no text:
// the reader recorded, per index and per exclusion constraint, which extensions
// that object's operator classes and access method belong to, so the answer is
// a catalog fact rather than a guess about a word. The two overlap rather than
// partition -- a printed operator class is both a name and a catalog edge, and
// the default class PostgreSQL declined to print is only the latter -- so the
// caller asks the name question first and reaches this one for what no name
// covers. An object this render leaves out takes its requirement with it --
// whether a selector dropped it or [renderTables] found no table to write it
// into -- which is why the edge is carried on the object and not on the
// extension.
func (r *renderer) documentRequiresExtension(name string) bool {
	if r.requiredExtensions == nil {
		r.requiredExtensions = collectRequiredExtensions(r.db)
	}
	return r.requiredExtensions[strings.ToLower(strings.TrimSpace(name))]
}

// collectRequiredExtensions gathers every extension an index or constraint
// THIS RENDER WRITES resolves to without naming.
//
// Only indexes and constraints carry the edge, because pg_index is where an
// operator class is resolved: a plain index, and the index PostgreSQL builds
// under an exclusion constraint, are the two shapes that reach a rendered
// document. A primary key or a single-column unique constraint is backed by an
// index too, but its operator class is the default for the column's own type,
// so an extension can only be behind it by supplying a default class for a type
// it does not supply -- measured over the 45 extensions in the postgres:17
// image, the extension-supplied default classes on core types are all gin, gist
// or bloom classes, and none is btree. Should one appear, its column would have
// to be a core type and the edge would have to be carried on the table.
//
// Being in db.Indexes is not the same as being in the document, which is why
// each object is asked the question [renderTables] asks -- through the same
// [resolveTable], so the two cannot answer differently. An index whose table
// this render does not write is reported as an orphan and emitted nowhere, so
// counting it keeps an extension block for an index the file does not contain,
// and the pinned Atlas community binary v1.3.0 refuses ANY postgres file
// declaring an extension block. That is the false positive [documentNamesAny]
// names: the whole result this suppression exists to produce, spent on nothing.
// The shape is ordinary rather than hypothetical -- a materialized view's index
// is read from pg_index with its resolved operator classes and can never be
// rendered, because a `materialized` block carries no index. Measured on
// PostgreSQL 17.10 (stokaro/ptah#1286):
//
//	CREATE EXTENSION btree_gin;
//	CREATE TABLE src (id integer PRIMARY KEY, n integer NOT NULL);
//	CREATE MATERIALIZED VIEW mv AS SELECT id, n FROM src;
//	CREATE INDEX mv_gin ON mv USING gin (n int4_ops);
//
// emits a document with no index block anywhere; kept, the pinned binary
// refused it with `postgres: extensions are not supported by this version`,
// while both documents applied at exit 0.
func collectRequiredExtensions(db *goschema.Database) map[string]bool {
	required := map[string]bool{}
	add := func(names []string) {
		for _, name := range names {
			name = strings.ToLower(strings.TrimSpace(name))
			if name != "" {
				required[name] = true
			}
		}
	}
	for _, index := range db.Indexes {
		if resolveTable(db.Tables, index.StructName, index.TableName) == nil {
			continue
		}
		add(index.RequiresExtensions)
	}
	for _, constraint := range db.Constraints {
		if resolveTable(db.Tables, constraint.StructName, constraint.Table) == nil {
			continue
		}
		add(constraint.RequiresExtensions)
	}
	return required
}

// documentNamesAny reports whether anything the surviving document emits names
// any of these identifiers.
//
// Matching is case-insensitive because unquoted PostgreSQL identifiers are.
//
// Neither direction of a wrong answer is safe, so this does not lean either
// way. A false negative emits a document with a hole in it: the Atlas community
// CLI refuses it and so does everything else, because the document names an
// object nothing declares. A false positive emits a correct document carrying
// an extension block, and that binary refuses ANY schema file declaring one --
// measured, every kept block is a refusal, not a partial loss. So a spurious
// match costs the entire result this suppression exists to produce, on a schema
// with no relationship to the extension (stokaro/ptah#1280).
//
// What makes the answer precise is therefore the input: the names in
// [goschema.Extension.Provides] are only those the extension supplies that do
// not also resolve without it, and among functions a keyword-named one is left
// out only where a type of the same extension is in the list and stands in for
// it. This scan reads words, not positions, so `DELETE FROM audit` in a plpgsql
// body is the same token as a call to hstore's `delete`, and a database using
// no hstore was carrying an hstore block because of it (stokaro/ptah#1281).
// Where no type stands in, the keyword-shaped name is all this scan has and the
// reader keeps it -- dropping it would cost a document its only declaration of
// what it calls.
func (r *renderer) documentNamesAny(names []string) bool {
	if r.references == nil {
		r.references = collectReferencedNames(r.db)
	}
	for _, name := range names {
		name = strings.ToLower(strings.TrimSpace(name))
		if name != "" && r.references[name] {
			return true
		}
	}
	return false
}

// collectReferencedNames gathers every identifier named by the parts of an
// inspected document that survive suppression.
//
// The candidate blocks' own bodies are deliberately NOT scanned. A sequence's
// `owned_by`, an extension's `version`, and a policy's `using` expression only
// exist in the output when that block does, so counting them would keep a block
// alive on a reference that goes away with it.
//
// Type positions are scanned alongside expressions because a dependency on an
// extension shows up as the type of a column -- `isbn`, `citext`, `hstore` --
// as often as it shows up inside an expression.
//
// This builds the set of names the document USES. Deciding whether a particular
// extension is still needed is the other half, and it belongs to
// [goschema.Extension.Provides], which the PostgreSQL reader fills from
// pg_depend: the names collected here are matched against what the extension
// supplies, not against its label.
func collectReferencedNames(db *goschema.Database) map[string]bool {
	names := map[string]bool{}
	add := func(text string) {
		for _, token := range sqlIdentifierTokens(text) {
			names[token] = true
		}
	}

	for _, field := range db.Fields {
		add(field.Type)
		add(field.DefaultExpr)
		add(field.UpdateExpression)
		add(field.GeneratedExpression)
		add(field.Check)
		add(field.UniqueExpr)
	}
	for _, table := range db.Tables {
		add(table.CustomSQL)
		for _, check := range table.Checks {
			add(check)
		}
		if table.Partition != nil {
			for _, part := range table.Partition.Parts {
				add(part.Expr)
			}
		}
	}
	for _, constraint := range db.Constraints {
		add(constraint.CheckExpression)
		add(constraint.WhereCondition)
		add(constraint.ExcludeElements)
	}
	for _, index := range db.Indexes {
		add(index.Condition)
		// An operator class is rendered as `ops`, on the index or on one of its
		// parts, and it names something only an extension may declare:
		// `ops = "gin_trgm_ops"` needs pg_trgm and nothing else in such a
		// document says so. A class reaches the document exactly when it is not
		// the default for its key's type, and a non-default extension class is
		// one the shadowed-name filter keeps in
		// [goschema.Extension.Provides], so what lands here is matchable. Not
		// reading it omitted pg_trgm from a document whose index block spelled
		// gin_trgm_ops, and the document then failed to apply with `operator
		// class "gin_trgm_ops" does not exist for access method "gin"` --
		// measured on PostgreSQL 17.10 (stokaro/ptah#1286). The constraint arm
		// has always been read, through ExcludeElements above, which is where
		// the same class is printed for `EXCLUDE USING gist (txt gist_trgm_ops
		// WITH =)`.
		add(index.Operator)
		for _, part := range index.Parts {
			add(part.Expr)
			add(part.Operator)
		}
	}
	for _, view := range db.Views {
		add(view.Body)
	}
	for _, view := range db.MaterializedViews {
		add(view.Body)
	}
	for _, function := range db.Functions {
		add(function.Body)
		add(function.Parameters)
		add(function.Returns)
	}
	for _, trigger := range db.Triggers {
		add(trigger.Body)
	}
	for _, domain := range db.Domains {
		add(domain.BaseType)
		add(domain.DefaultExpr)
		add(domain.Check)
	}
	for _, composite := range db.CompositeTypes {
		for _, field := range composite.Fields {
			add(field.Type)
		}
	}
	for _, rangeType := range db.Ranges {
		add(rangeType.Subtype)
		add(rangeType.Canonical)
		add(rangeType.SubtypeDiff)
	}
	// A grant's target is an HCL traversal in the rendered `permission` block,
	// so an omitted target leaves a reference to a block that is not there.
	// Measured on PostgreSQL 17: a GRANT on a sequence arrives with the
	// sequence in OnTable rather than OnSequence, so both are read here.
	for _, grant := range db.Grants {
		add(grant.OnTable)
		add(grant.OnSequence)
	}
	return names
}

// sqlIdentifierTokens splits text into lowercased identifier-shaped words.
//
// The split is deliberately naive: `nextval('order_seq'::regclass)` has to
// yield `order_seq` through a quote and a cast, and a SQL parser per dialect
// would be a much larger thing to keep correct than a rule whose failure mode
// is keeping a block that did not have to be kept.
//
// The words it yields carry no position, so a statement keyword and a call to a
// function of the same name are the same token here. That is answered where the
// member list is built rather than guessed at afterwards: the PostgreSQL reader
// leaves a keyword-named function out of [goschema.Extension.Provides] when a
// type of the same extension is in that list to answer for it, and keeps it
// when none is.
func sqlIdentifierTokens(text string) []string {
	var tokens []string
	var current strings.Builder
	flush := func() {
		if current.Len() > 0 {
			tokens = append(tokens, strings.ToLower(current.String()))
			current.Reset()
		}
	}
	for _, r := range text {
		switch {
		case r == '_' || r == '$' ||
			r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9':
			current.WriteRune(r)
		default:
			flush()
		}
	}
	flush()
	return tokens
}
