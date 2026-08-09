package atlashcl

import (
	"fmt"
	"strings"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/envbool"
)

// One HCL document is a set of declarations, and declaring one object twice in
// it is a mistake rather than an instruction to merge.
//
// [goschema.Finalize] folds a repeated declaration into the first one it saw.
// That fold is load-bearing for the NATIVE Go-annotation path and must stay:
// a package whose files each carry `//ptah:schema:table name="users"` is one
// table, and disabling the fold reddens `TestParseDir_Deduplication`,
// `TestParseFS_Deduplication` and 15 further tests in core/goschema. So the
// refusal belongs here, where a single DOCUMENT is parsed, and not in the
// shared folding step.
//
// # What decides whether a kind is refused by default
//
// One rule, applied to every kind, and every row of it is a measurement rather
// than a judgement: a repeat is refused BY DEFAULT exactly where the pinned
// Atlas community binary v1.3.0 refuses the same document. That is the drop-in
// floor -- never exit 0 where it exits 1 -- and nothing else belongs in the
// default. A kind that binary reads at exit 0 is accepted at exit 0 here too,
// and the stricter answer for it lives behind [StrictRedeclarationsEnvVar].
//
// Measured on PostgreSQL 17.10 with a throwaway dev database dropped and
// recreated between every run, `schema inspect -u file://<fixture>.hcl
// --dev-url postgres://…`, exit codes read from unpiped invocations:
//
//	fixture                        pinned binary v1.3.0                     before this file
//	table "users" twice            exit 1  pq: relation "users" already      exit 0, one table
//	                                       exists (42P07)
//	enum "mood" twice              exit 1  duplicate enum "mood"             exit 0, one enum
//	index "idx_users_id" twice     exit 1  pq: relation "idx_users_id"       exit 0, one index
//	                                       already exists (42P07)
//	column "id" twice              exit 1  pq: column "id" specified more    exit 0, one column
//	                                       than once (42701)
//	check "id_positive" twice      exit 1  pq: check constraint              exit 0, one check
//	                                       "id_positive" already exists (42710)
//	foreign_key "posts_author_fk"  exit 1  pq: constraint "posts_author_fk"  exit 0, one key
//	  twice, one column                    for relation "posts" already
//	                                       exists (42710)
//	unique "users_email_key" twice exit 0  merged, one unique block out      exit 0, one unique
//	primary_key twice              exit 0  merged, one primary_key out       exit 0, one key
//	row_security twice             exit 0  block dropped unread              exit 0
//	variable "tenant" twice        exit 0  merged, default substituted       exit 0
//	view/materialized/role twice   exit 0  block dropped unread              exit 0
//
// The `foreign_key` row is the one this ledger was extended for. A
// SINGLE-column foreign key is not a [goschema.Constraint] at all -- see
// [parser.applyForeignKey], which writes it onto the referencing FIELD -- so
// walking `db.Constraints` could never see it, and the block declared twice was
// applied twice to one field and rendered once. `documentDeclarations` reads the
// blocks the parser recorded instead, which is the only place both spellings of
// a foreign key are one thing.
//
// This is the within-file half of a rule the directory loader already applies
// across files: see internal/schemafile/schemadir_order.go, which refuses a
// file that declares an object an earlier file in the same directory declared.
// The `schema` and `function` exemptions below are that ledger's exemptions as
// well, for the same two reasons.
//
// [MergeRedeclarationsEnvVar] restores the merge, because the merge is a
// capability rather than a bug -- it is how the Go-annotation path reads one
// entity seen in several files -- and compatibility never removes one.

// MergeRedeclarationsEnvVar restores the pre-refusal behavior for HCL schema
// documents: a repeated declaration folds into the first one instead of
// refusing the document.
//
// It is an environment variable rather than a flag because the conformance
// cli-surface tier asserts flag parity with the pinned Atlas community binary
// v1.3.0, so a new flag would fail it.
const MergeRedeclarationsEnvVar = "PTAH_HCL_MERGE_REDECLARATIONS"

// StrictRedeclarationsEnvVar refuses a repeated declaration of the kinds the
// pinned Atlas community binary v1.3.0 reads at exit 0: `view`, `materialized`,
// `role` and `unique`.
//
// The default is parity, and that is a correction rather than a preference.
// Measured on PostgreSQL 17.10, that binary reads a document repeating any of
// those four at exit 0 -- it drops `view`, `materialized` and `role` unread, and
// merges `unique` into one block -- and so did Ptah before the refusal existed.
// A refusal there is not the drop-in floor, it is above it, and the
// compatibility surface defaults to the floor (AGENTS.md).
//
// The stricter answer is still worth having: Ptah MODELS all four, and the
// directory loader in internal/schemafile/schemadir_order.go already refuses a
// repeat of each ACROSS files. This variable makes the within-file rule the same
// rule, for an operator who wants a document's mistakes named rather than
// merged.
const StrictRedeclarationsEnvVar = "PTAH_HCL_STRICT_REDECLARATIONS"

// SchemaScopedEnumsEnvVar keys `enum` blocks by their SCHEMA-QUALIFIED name, so
// one enum name declared in two schemas is two objects rather than one declared
// twice.
//
// The default is the bare name, and it is a parity floor rather than a model of
// what an enum is. The pinned Atlas community binary v1.3.0 keys enums by their
// bare name: measured on PostgreSQL 17.10, `enum "mood"` in schema public
// alongside `enum "mood"` in schema other is `Error: duplicate enum "mood"`,
// exit 1, and so is the two-label spelling `enum "public" "mood"` alongside
// `enum "other" "mood"`. Reading that document at exit 0 is exactly the
// direction the drop-in rule forbids.
//
// That binary cannot read its own inspect output for such a realm, measured:
// its `schema inspect` of a database holding public.mood and other.mood emits
// both as two-label blocks and then refuses the file it just wrote. Ptah's IR
// does model both, its renderer writes the same two-label spelling, and with
// this variable set Ptah reads the document back and re-renders it byte for
// byte -- the round trip that binary does not have. See
// [go.5x5.cz/ptah/internal/atlashclrender] for the rendering half.
const SchemaScopedEnumsEnvVar = "PTAH_HCL_SCHEMA_SCOPED_ENUMS"

// The declarations of the three variables, made once, in the package that owns
// them. See [go.5x5.cz/ptah/internal/envbool].
var (
	mergeRedeclarationsVar  = envbool.New(MergeRedeclarationsEnvVar, false)
	strictRedeclarationsVar = envbool.New(StrictRedeclarationsEnvVar, false)
	schemaScopedEnumsVar    = envbool.New(SchemaScopedEnumsEnvVar, false)
)

// redeclarationPolicy is the three variables resolved once per parse, so one
// document cannot be read under two different rules.
type redeclarationPolicy struct {
	merge             bool
	strict            bool
	schemaScopedEnums bool
}

func resolveRedeclarationPolicy() (redeclarationPolicy, error) {
	merge, err := mergeRedeclarationsVar.Resolve()
	if err != nil {
		return redeclarationPolicy{}, err
	}
	strict, err := strictRedeclarationsVar.Resolve()
	if err != nil {
		return redeclarationPolicy{}, err
	}
	schemaScopedEnums, err := schemaScopedEnumsVar.Resolve()
	if err != nil {
		return redeclarationPolicy{}, err
	}
	return redeclarationPolicy{merge: merge, strict: strict, schemaScopedEnums: schemaScopedEnums}, nil
}

// declaredObject is one object a document declares, and its two fields are what
// make two declarations the same declaration.
//
// They are kept apart rather than joined into one string because every name
// here is an SQL identifier and quoting lets one contain the separator, which is
// the encoding mistake [goschema.Deduplicate] documents at
// deduplicateNamedDefinitions.
//
// There is deliberately NO case fold on the name, and that is the whole point of
// this comment. An HCL label reaches DDL quoted, so `table "users"` and
// `table "Users"` are two relations rather than two spellings of one: measured
// on PostgreSQL 17.10, a document declaring both is exit 0 on the pinned Atlas
// community binary v1.3.0 and its inspect output holds two tables. A folded key
// refuses that document -- the same mistake, in the same direction, that
// stokaro/ptah#1311 records for row-level security targets. Matching exactly can
// only fail to refuse something; folding refuses documents both binaries accept.
//
// The name is also the spelling the refusal prints, so a reader is shown the
// identity that collided rather than a normalized one.
type declaredObject struct {
	// kind is the word the refusal uses for this object.
	kind string
	// name is the object's name, qualified and spelled exactly as the document
	// declared it.
	name string
}

// declare is the ordinary ledger entry: an identity with nothing finer behind
// it, so a collision is a redeclaration and nothing else.
func declare(kind, name string) declaration {
	return declaration{object: declaredObject{kind: kind, name: name}}
}

// Object kinds, spelled the way the refusal names them.
const (
	kindTable            = "table"
	kindColumn           = "column"
	kindIndex            = "index"
	kindConstraint       = "constraint"
	kindForeignKey       = "foreign key"
	kindUnique           = "unique constraint"
	kindEnum             = "enum"
	kindDomain           = "domain"
	kindCompositeType    = "composite type"
	kindRange            = "range"
	kindSequence         = "sequence"
	kindExtension        = "extension"
	kindView             = "view"
	kindMaterializedView = "materialized view"
	kindTrigger          = "trigger"
	kindPolicy           = "policy"
	kindRole             = "role"
)

// declaration is one entry of the ledger: the identity two declarations collide
// on, plus what tells them apart when a finer identity Ptah models would not
// have collided at all.
//
// The second half is what makes the refusal actionable rather than merely
// correct. A document holding `enum "public" "mood"` and `enum "other" "mood"`
// collides under the bare-name identity the pinned Atlas community binary
// v1.3.0 uses, and the only honest advice for it is the variable that reads the
// two apart -- not the one that merges them, which would delete a type.
type declaration struct {
	object declaredObject
	// scopedName is the identity under the FINER rule, when Ptah has one. Two
	// entries with the same object and different scopedName are two objects the
	// default rule cannot tell apart.
	scopedName string
	// remedy names the environment variable that selects the finer rule.
	remedy string
}

// rejectRedeclarations refuses a document that declares one object twice.
//
// It runs after the body walk and before [goschema.Finalize], because Finalize
// is what makes the second declaration invisible.
func (p *parser) rejectRedeclarations() error {
	policy, err := resolveRedeclarationPolicy()
	if err != nil {
		return err
	}
	if policy.merge {
		return nil
	}
	seen := make(map[declaredObject]declaration)
	for _, current := range p.documentDeclarations(policy) {
		previous, exists := seen[current.object]
		if !exists {
			seen[current.object] = current
			continue
		}
		if current.remedy != "" && previous.scopedName != current.scopedName {
			return fmt.Errorf(
				"parse HCL schema %s: %s %q is declared more than once; "+
					"%q and %q are two objects Ptah models and one object on the "+
					"Atlas-compatible surface, which keys this kind by its bare name "+
					"(set %s=1 to read them as two)",
				p.filename, current.object.kind, current.object.name,
				previous.scopedName, current.scopedName, current.remedy,
			)
		}
		// The consequence is spelled as the two it can be rather than as one:
		// Finalize drops the repeat for the kinds it folds, and the engine
		// refuses the second CREATE for the kinds it does not.
		return fmt.Errorf(
			"parse HCL schema %s: %s %q is declared more than once; "+
				"a document declares each object once, and a repeat is either dropped in "+
				"silence or refused by the database "+
				"(set %s=1 to merge repeated declarations instead)",
			p.filename, current.object.kind, current.object.name, MergeRedeclarationsEnvVar,
		)
	}
	return nil
}

// documentDeclarations lists the named objects one parsed document declares, in
// declaration order.
//
// Each kind is keyed by the identity [goschema.Deduplicate] already folds that
// kind by, and that pairing is the point rather than a coincidence: the defect
// being closed is the fold's silent drop, so a COARSER key here would refuse a
// pair the fold keeps apart, and a FINER one would leave a drop unreported.
//
// Where the fold resolves a reference and this list compares raw text -- indexes
// and row-level security policies name their table through a resolver -- this
// list is the finer of the two. That is the safe direction: it can only fail to
// refuse a pair the fold folds, never refuse a pair it keeps.
//
// # The complete enumeration, one verdict per repeatable block kind
//
// Every block kind this parser accepts, at every nesting level it accepts one,
// with the verdict and the reason. A kind absent from this comment is a kind
// this parser does not accept.
//
// TOP LEVEL. `table`, `enum`, `sequence`, `domain`, `composite`, `range`,
// `extension`, `trigger` and `policy` are refused by default. `view`,
// `materialized` and `role` are refused only under
// [StrictRedeclarationsEnvVar], because the pinned binary reads a repeat of each
// at exit 0 -- it drops the block unread -- and so did Ptah. Four are exempt
// under every setting:
//
//   - `schema`. A repeated `schema "public" {}` is exit 0 on the pinned Atlas
//     community binary v1.3.0, measured on PostgreSQL 17.10, so refusing it
//     would refuse a document that binary reads. It is also the layout of an
//     HCL schema DIRECTORY, whose files each open with the same schema block --
//     the reason internal/schemafile/schemadir_order.go leaves schemas out of
//     its HCL ledger too. Which schemas a document may declare is a different
//     rule, decided against the run's URL scope in internal/schemafile
//     (stokaro/ptah#1231).
//   - `function`. A PostgreSQL function's identity includes its argument types,
//     so two blocks sharing a name can be two legal overloads. Keying them by
//     name would refuse a document PostgreSQL accepts. The same exemption, for
//     the same reason, is written down in schemadir_order.go.
//   - `permission`. It renders GRANT, which PostgreSQL accepts twice without
//     error, so a repeat is not a redeclaration.
//   - `data`. A managed-data block declares no database object; two of them
//     against one table are two loads, not one object declared twice.
//
// `env` is refused as a whole file before this ledger is built, and `variable`
// and `locals` are consumed by the evaluation context: measured, the pinned
// binary reads a document declaring `variable "tenant"` twice at exit 0 and
// substitutes the value, so a repeat there is parity and not a divergence.
//
// INSIDE A TABLE. `column`, `index` and a named `check` or `constraint` are
// refused by default; `foreign_key` joins them here for the first time.
// `unique` is refused only under [StrictRedeclarationsEnvVar], because the
// pinned binary merges two `unique` blocks sharing a label into one and exits 0.
// Three more are exempt:
//
//   - `primary_key` carries no name, and the pinned binary merges two of them
//     into one at exit 0 (measured). A table's second primary key is a
//     structural mistake this ledger is the wrong place to name.
//   - `row_security` carries no name either, and the pinned binary drops it
//     unread at exit 0. Two of them enable the same thing twice, which
//     PostgreSQL accepts.
//   - `platform` is Ptah's own dialect-override block, and it already refuses a
//     duplicated override key inside one dialect with its own error, so a
//     collision is named where the keys are.
//
// INSIDE A COLUMN. `as`, `identity` and `platform`. A repeated `as` or
// `identity` is already refused by the parser itself -- "column can contain at
// most one identity block" -- which is stricter than the pinned binary, which
// merges them at exit 0. That predates this ledger and is not changed here;
// adding a row for it would only duplicate an existing refusal.
//
// INSIDE AN INDEX, A PRIMARY KEY OR A PARTITION. `on` and `by` are positional
// list elements naming a column or an expression, not named objects: two of them
// are two columns, which is what a multi-column index IS.
//
// INSIDE A TRIGGER. `before`, `after` and `instead_of` are already refused as a
// group by "trigger contains multiple timing blocks".
//
// INSIDE A FUNCTION. `arg` is one parameter of a function whose whole block is
// exempt above.
//
// INSIDE A COMPOSITE. `field` is one attribute of a composite type whose whole
// block is refused above; the pinned binary refuses any document declaring a
// composite for a construct-level reason, so no exit-code divergence inside one
// is attributable to a repeat.
//
// INSIDE A PLATFORM. `override` already refuses a duplicated key for one dialect
// with its own error.
func (p *parser) documentDeclarations(policy redeclarationPolicy) []declaration {
	db := p.db
	if db == nil {
		return nil
	}

	objects := make([]declaration, 0, len(db.Tables)+len(db.Fields)+len(db.Indexes))
	for _, table := range db.Tables {
		objects = append(objects, declare(kindTable, table.QualifiedName()))
	}
	// A column is named inside its table, so the table is part of its identity.
	for _, field := range db.Fields {
		objects = append(objects, declare(kindColumn, qualifyWithOwner(field.StructName, field.Name)))
	}
	// Index and constraint names are unique per TABLE on MySQL and MariaDB, so
	// two tables each carrying an `idx_name` is an ordinary layout rather than a
	// collision.
	for _, index := range db.Indexes {
		objects = append(objects, declare(kindIndex, qualifyWithOwner(indexOwner(index), index.Name)))
	}
	objects = append(objects, declaredConstraints(db, policy)...)
	objects = append(objects, p.declaredForeignKeys...)
	objects = append(objects, declaredTypes(db, policy)...)
	for _, sequence := range db.Sequences {
		objects = append(objects, declare(kindSequence, sequence.QualifiedName()))
	}
	for _, extension := range db.Extensions {
		objects = append(objects, declare(kindExtension, extension.Name))
	}
	for _, trigger := range db.Triggers {
		objects = append(objects, declare(kindTrigger, qualifyWithOwner(trigger.Table, trigger.Name)))
	}
	for _, policyBlock := range db.RLSPolicies {
		objects = append(objects, declare(kindPolicy, qualifyWithOwner(policyOwner(policyBlock), policyBlock.Name)))
	}
	return append(objects, declaredBeyondParity(db, policy)...)
}

// declaredBeyondParity lists the kinds the pinned Atlas community binary v1.3.0
// reads at exit 0 when they are repeated.
//
// They are one function rather than four loops among the others so the
// difference between "the drop-in floor" and "above it" is a single call the
// reader can see, and so the mutant that moves a kind across the line has one
// place to move it.
func declaredBeyondParity(db *goschema.Database, policy redeclarationPolicy) []declaration {
	if !policy.strict {
		return nil
	}
	objects := make([]declaration, 0, len(db.Views)+len(db.MaterializedViews)+len(db.Roles))
	for _, view := range db.Views {
		objects = append(objects, declare(kindView, view.Name))
	}
	for _, view := range db.MaterializedViews {
		objects = append(objects, declare(kindMaterializedView, view.Name))
	}
	for _, role := range db.Roles {
		objects = append(objects, declare(kindRole, role.Name))
	}
	return objects
}

// declaredConstraints lists the table-level constraints a document declares.
//
// A UNIQUE constraint is held back to the beyond-parity set: the pinned binary
// merges two `unique` blocks sharing a label into one and exits 0 (measured,
// PostgreSQL 17.10), unlike a repeated named `check`, which reaches the engine
// twice and is refused with `check constraint "id_positive" already exists`.
// The two live in one Go slice and must not therefore live under one verdict.
//
// A FOREIGN KEY constraint is skipped here and counted by
// [parser.declaredForeignKeys] instead, because only ONE of the two shapes a
// `foreign_key` block can take lands in this slice.
func declaredConstraints(db *goschema.Database, policy redeclarationPolicy) []declaration {
	objects := make([]declaration, 0, len(db.Constraints))
	for _, constraint := range db.Constraints {
		// Every constraint this parser produces carries a name -- an unlabeled
		// `check` block is named after its table and its ordinal -- so a nameless
		// one is a future parser's mistake rather than a document's. Skipping it
		// keeps that mistake from reading as "two objects with the same empty
		// name" and refusing a document nobody wrote wrong.
		name := strings.TrimSpace(constraint.Name)
		if name == "" {
			continue
		}
		kind := kindConstraint
		switch strings.ToUpper(strings.TrimSpace(constraint.Type)) {
		case "FOREIGN KEY":
			continue
		case "UNIQUE":
			if !policy.strict {
				continue
			}
			kind = kindUnique
		}
		objects = append(objects, declare(kind, qualifyWithOwner(constraintOwner(constraint), name)))
	}
	return objects
}

// recordForeignKey notes one `foreign_key` block as the document declared it.
//
// It is recorded HERE, at the block, rather than read back out of the IR,
// because a foreign key has two landing places and only one of them is a
// [goschema.Constraint]: [parser.applyForeignKey] puts a SINGLE-column key on
// the referencing field, where nothing distinguishes "declared once" from
// "declared twice and applied twice". Measured on PostgreSQL 17.10, a document
// declaring one single-column `foreign_key "posts_author_fk"` twice is
// `Error: create "posts" table: pq: constraint "posts_author_fk" for relation
// "posts" already exists (42710)` at exit 1 on the pinned Atlas community binary
// v1.3.0, and was exit 0 here with one key rendered.
//
// The identity is the table plus the constraint name, which is what PostgreSQL
// collided on in that message and what a constraint name is scoped by.
func (p *parser) recordForeignKey(table goschema.Table, name string) {
	name = strings.TrimSpace(name)
	if name == "" {
		return
	}
	p.declaredForeignKeys = append(
		p.declaredForeignKeys,
		declare(kindForeignKey, qualifyWithOwner(table.QualifiedName(), name)),
	)
}

// declaredTypes lists the four block types that create a PostgreSQL type.
//
// Domains, composite types and ranges are keyed by their qualified name, because
// that is what the composite finalization folds them by.
//
// The enum key is the one that moves. By default it is the BARE name, matching
// both [goschema.Deduplicate] and the pinned Atlas community binary v1.3.0,
// which refuses `enum "mood"` in schema public alongside `enum "mood"` in schema
// other with `duplicate enum "mood"` (measured, PostgreSQL 17.10, realm-scoped
// dev URL). Reading that document at exit 0 is the direction the drop-in rule
// forbids, so the bare name is the default. [SchemaScopedEnumsEnvVar] selects
// the qualified name, which is what the two objects actually are and what makes
// Ptah's own two-schema inspect output readable again.
func declaredTypes(db *goschema.Database, policy redeclarationPolicy) []declaration {
	objects := make([]declaration, 0,
		len(db.Enums)+len(db.Domains)+len(db.CompositeTypes)+len(db.Ranges))
	for _, enum := range db.Enums {
		objects = append(objects, declaredEnum(enum, policy))
	}
	for _, domain := range db.Domains {
		objects = append(objects, declare(kindDomain, domain.QualifiedName()))
	}
	for _, composite := range db.CompositeTypes {
		objects = append(objects, declare(kindCompositeType, composite.QualifiedName()))
	}
	for _, rangeType := range db.Ranges {
		objects = append(objects, declare(kindRange, rangeType.QualifiedName()))
	}
	return objects
}

// declaredEnum is the one ledger entry that carries a finer identity behind the
// one it collides on.
//
// Under the default the entry collides on the bare name and REMEMBERS the
// qualified one, so a collision between public.mood and other.mood can say what
// the two objects are and name the variable that reads them apart, instead of
// advising a merge that would delete a type. Under the variable the two
// identities are the same string, so the finer branch cannot fire and a real
// repeat still reports a real repeat.
func declaredEnum(enum goschema.Enum, policy redeclarationPolicy) declaration {
	entry := declare(kindEnum, enum.Name)
	if policy.schemaScopedEnums {
		return declare(kindEnum, enum.QualifiedName())
	}
	entry.scopedName = enum.QualifiedName()
	entry.remedy = SchemaScopedEnumsEnvVar
	return entry
}

// indexOwner names the table an index belongs to. This runs before
// [goschema.Finalize], so TableName is set only when the document said so and
// StructName carries the table the index block sat in.
func indexOwner(index goschema.Index) string {
	if strings.TrimSpace(index.TableName) != "" {
		return index.TableName
	}
	return index.StructName
}

// constraintOwner names the table a constraint belongs to, preferring an
// explicit table over the block it was written in -- the same preference
// [goschema.Deduplicate] applies at constraintDedupKey.
func constraintOwner(constraint goschema.Constraint) string {
	if table := strings.TrimSpace(constraint.Table); table != "" {
		return table
	}
	return constraint.StructName
}

// policyOwner names the table a row-level security policy belongs to.
func policyOwner(policy goschema.RLSPolicy) string {
	if table := strings.TrimSpace(policy.Table); table != "" {
		return table
	}
	return policy.StructName
}

// qualifyWithOwner prefixes a table-scoped object with its table.
func qualifyWithOwner(owner, name string) string {
	if owner == "" {
		return name
	}
	return owner + "." + name
}
