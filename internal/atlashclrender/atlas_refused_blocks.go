package atlashclrender

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"

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
func (r *renderer) omitRefusedBlock(path, block, name string) bool {
	if !r.omitAtlasRefusedBlocks || !atlasRefusesBlock(r.dialect, block) {
		return false
	}
	if r.documentNames(name) {
		r.warn(path, fmt.Sprintf(
			"kept in Atlas-compatible schema inspect output because another object in this document names it:"+
				" the Atlas community CLI refuses a %s schema file that declares any %s block, so it cannot read"+
				" this document, but omitting the block would leave a reference to an object nothing declares",
			r.dialect, block,
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

// documentNames reports whether anything the surviving document emits names
// this object.
//
// Matching is case-insensitive because unquoted PostgreSQL identifiers are, and
// because the direction of a wrong answer matters: a false positive keeps a
// block that could have been omitted, which costs compatibility; a false
// negative emits a document with a hole in it, which costs correctness. This
// errs toward keeping.
func (r *renderer) documentNames(name string) bool {
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" {
		return false
	}
	if r.references == nil {
		r.references = collectReferencedNames(r.db)
	}
	return r.references[name]
}

// collectReferencedNames gathers every identifier named by the parts of an
// inspected document that survive suppression.
//
// The candidate blocks' own bodies are deliberately NOT scanned. A sequence's
// `owned_by`, an extension's `version`, and a policy's `using` expression only
// exist in the output when that block does, so counting them would keep a block
// alive on a reference that goes away with it.
//
// Type positions are scanned alongside expressions because an extension that
// supplies a type is named by the columns using it -- `citext`, `hstore`. An
// extension that supplies only FUNCTIONS is not named anywhere by this rule:
// `pgcrypto` behind `gen_random_uuid()` is invisible to it, and that limitation
// is documented rather than papered over, because resolving it needs a catalog
// of what each extension provides rather than a name.
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
		for _, part := range index.Parts {
			add(part.Expr)
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
