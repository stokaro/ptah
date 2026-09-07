// Package renderdiag carries the record a renderer leaves when a target cannot
// hold something the schema declared, and the sink it writes that record to.
//
// Rendering answers a declaration in one of three ways. It emits the
// declaration, it refuses the whole render with an error, or it continues
// without the declaration. The third answer is the one nothing could see: a
// PostgreSQL-family target writes a `skipped` comment beside the statement,
// while SQLite, SQL Server and Oracle drop the same table options without a
// word, and both spellings exit 0 (stokaro/ptah#2976).
//
// A record here is what makes that answer readable by a program. It is not a
// finding and it is not published: `ptah schema validate --no-skipped` maps a
// record into the problem model that verb already reports, so this package adds
// a vocabulary rather than a fifth analysis result shape.
//
// A comment answered with a `-- text` line is recorded too. The server stores
// none of it, so the render looks like it kept the text while the database has
// none, which to the author is the same outcome as dropping it.
//
// # This is not coverage
//
// [ptah.run/core/coverage] answers a neighboring question and must not be
// reached for here. Coverage says a description never claimed to describe an
// object, so the comparator plans nothing for it. An omission says the author
// declared an object and this target will not carry it. Feeding one into the
// other would suppress the difference instead of reporting the loss.
package renderdiag

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"strings"

	"ptah.run/core/ast"
)

// Reason names why a declaration did not reach the output.
//
// The set is closed and each value is stable: a caller branches on it, and the
// sentence a renderer writes beside the record is free to change wording
// without changing what the caller decided.
type Reason string

// The reasons a renderer records.
const (
	// ReasonUnsupported marks a declaration the target has no representation
	// for. The declaration is well formed and another target renders it.
	ReasonUnsupported Reason = "unsupported"
)

// Omission is one declared thing a target did not render.
//
// Kind and Name identify the object that owned the declaration, so a reader can
// find it in the source. Property names what was lost from that object, and is
// empty when the whole object was. Detail carries the declared value where
// repeating it helps the reader recognize what went missing.
//
// The target is deliberately not a field. A sink belongs to one render of one
// target, and the entry point that owns the sink is the one place that already
// holds the normalized dialect name, so stamping it there keeps eight renderers
// from each spelling it again.
type Omission struct {
	// Reason is why the declaration did not reach the output.
	Reason Reason
	// Kind is the owning object's kind, such as "table".
	Kind string
	// Name is the owning object's name as the declaration spells it.
	Name string
	// Property names the lost property, empty when the whole object was lost.
	Property string
	// Detail is the declared value, empty when there is nothing to repeat.
	Detail string
	// Remedy states how to keep the declaration on this target. It is empty
	// unless a remedy exists here: a suggestion that does not work on the
	// target it is printed for costs the reader more than silence.
	Remedy string
}

// Sink collects omissions during one render.
//
// A renderer is built fresh for each statement on the ordered-render path, so
// the sink rather than the renderer is what spans a schema. A nil *Sink is
// usable and drops what it is given, which is what a render nobody asked to
// report runs with.
type Sink struct {
	omissions []Omission
}

// Record adds one omission. A nil sink ignores it.
func (s *Sink) Record(omission Omission) {
	if s == nil {
		return
	}
	s.omissions = append(s.omissions, omission)
}

// Omissions returns what was recorded, in a deterministic order.
//
// Records arrive in whatever order the walk reached the objects, and that order
// is an implementation detail of the lowering rather than something a reader
// should have to predict. Sorting by the identity a reader sees keeps a report
// stable across runs and across changes to the walk.
func (s *Sink) Omissions() []Omission {
	if s == nil || len(s.omissions) == 0 {
		return nil
	}
	out := slices.Clone(s.omissions)
	slices.SortStableFunc(out, func(a, b Omission) int {
		return cmp.Or(
			cmp.Compare(a.Kind, b.Kind),
			cmp.Compare(a.Name, b.Name),
			cmp.Compare(a.Property, b.Property),
			cmp.Compare(a.Detail, b.Detail),
		)
	})
	return out
}

// The vocabulary for a table option a target does not carry.
//
// Five renderers drop these and each one would otherwise spell the record its
// own way, which is the shape a reader has to normalize before they can compare
// two targets. One constructor is what keeps `table option ENGINE` from also
// being `ENGINE option` and `engine`.
const (
	// TableKind is the owning object's kind for a table-level loss.
	TableKind = "table"
	// TableOptionProperty prefixes the property name of a table option.
	TableOptionProperty = "table option"
)

// TableOptionOmission is the record for one table option a target drops.
//
// remedy is empty unless keeping the declaration is possible on that target;
// the option key alone does not decide that, because the answer differs per
// family.
func TableOptionOmission(table, key, value, remedy string) Omission {
	return Omission{
		Reason:   ReasonUnsupported,
		Kind:     TableKind,
		Name:     table,
		Property: TableOptionProperty + " " + key,
		Detail:   value,
		Remedy:   remedy,
	}
}

// RecordDroppedTableOptions records every option the renderer did not consume.
//
// kept names the keys the caller rendered, compared without case because the
// same option reaches a renderer spelled both `WITHOUT_ROWID` and
// `WITHOUT ROWID`. Recording the complement rather than a list of losses is
// what keeps a renderer honest when a new option key appears: an option nobody
// taught it to render is reported rather than silently new.
func (s *Sink) RecordDroppedTableOptions(table string, options map[string]string, kept ...string) {
	if s == nil {
		return
	}
	for _, key := range slices.Sorted(maps.Keys(options)) {
		if slices.ContainsFunc(kept, func(k string) bool { return strings.EqualFold(k, key) }) {
			continue
		}
		s.Record(TableOptionOmission(table, key, options[key], ""))
	}
}

// Kinds an omission names beside [TableKind].
const (
	// ColumnKind owns a property lost from one column. Its Name is
	// `table.column`, because a column name alone does not identify one.
	ColumnKind = "column"
	// IndexKind owns a property lost from one index.
	IndexKind = "index"
)

// CommentProperty is the property name a lost comment is recorded under.
//
// A comment is the one declaration several targets answer with a SQL line
// comment, which the server does not store: the render looks like it kept the
// text and the database has none of it. Recording it under one name keeps
// `-- text` and dropping the text outright from reading as two different
// outcomes, because to the author they are the same one.
const CommentProperty = "comment"

// PropertyOmission is the record for one property an object lost.
func PropertyOmission(kind, name, property, value string) Omission {
	return Omission{
		Reason:   ReasonUnsupported,
		Kind:     kind,
		Name:     name,
		Property: property,
		Detail:   value,
	}
}

// RecordLostComment records a comment the target does not store. An empty
// comment records nothing, because nothing was declared to lose.
func (s *Sink) RecordLostComment(kind, name, comment string) {
	if s == nil || comment == "" {
		return
	}
	s.Record(PropertyOmission(kind, name, CommentProperty, comment))
}

// ColumnName is the identity a column omission carries.
func ColumnName(table, column string) string {
	return table + "." + column
}

// Index properties a target can decline to carry.
const (
	// ConditionProperty is a partial index's WHERE condition. Losing it is not
	// a cosmetic loss: the index is created over every row instead of the
	// declared subset, and on a unique index that changes which rows the
	// database accepts.
	ConditionProperty = "partial index condition"
	// OperatorClassProperty is a PostgreSQL operator class. It is declared per
	// index or per part, and only the PostgreSQL family has a clause for it.
	OperatorClassProperty = "operator class"
)

// RecordLostProperty records a declared property the target does not carry.
// An empty value records nothing, because nothing was declared to lose.
func (s *Sink) RecordLostProperty(kind, name, property, value string) {
	if s == nil || value == "" {
		return
	}
	s.Record(PropertyOmission(kind, name, property, value))
}

// Identity clause properties a target can decline to carry.
const (
	// IdentityGenerationProperty is ALWAYS or BY DEFAULT. Oracle renders BY
	// DEFAULT whatever was declared, so losing this one substitutes a
	// semantics rather than omitting a clause.
	IdentityGenerationProperty = "identity generation"
	// IdentityStartProperty is the first value the key takes. It is the value
	// a PostgreSQL-family target tells the author to move an AUTO_INCREMENT
	// start onto (stokaro/ptah#2969), so a target that drops it silently makes
	// that advice wrong.
	IdentityStartProperty = "identity start"
	// IdentityIncrementProperty is the step between generated values.
	IdentityIncrementProperty = "identity increment"
	// IdentityOptionsProperty is the raw sequence option list.
	IdentityOptionsProperty = "identity options"
)

// Identity carries the identity clauses one column declares. A zero field is a
// clause the column did not declare, and records nothing.
type Identity struct {
	Generation string
	Start      string
	Increment  string
	Options    string
}

// RecordLostIdentity records the identity clauses a target does not carry.
//
// The caller passes only what its renderer drops, so a target that writes the
// start and increment leaves those zero. Every generated key ends up spelled
// some way on every target -- AUTO_INCREMENT, AUTOINCREMENT, IDENTITY -- and
// that spelling is not the loss; the values the author chose are.
func (s *Sink) RecordLostIdentity(name string, lost Identity) {
	if s == nil {
		return
	}
	for _, declared := range []struct {
		property string
		value    string
	}{
		{IdentityGenerationProperty, lost.Generation},
		{IdentityIncrementProperty, lost.Increment},
		{IdentityOptionsProperty, lost.Options},
		{IdentityStartProperty, lost.Start},
	} {
		s.RecordLostProperty(ColumnKind, name, declared.property, declared.value)
	}
}

// Column properties a target can decline to carry.
//
// These are the per-column declarations that reach a renderer and leave no
// trace: the render emits the column, exits 0, and the property is gone. Each
// is named for what the author wrote rather than for the clause a particular
// target would have used, so one report reads the same across six targets.
const (
	// CharsetProperty is a column's character set. Only the MySQL family has a
	// per-column clause for it.
	CharsetProperty = "character set"
	// CollateProperty is a column's collation.
	//
	// Losing it changes which rows a comparison and a unique index treat as
	// equal, so it is the one of this group with a consequence beyond storage.
	// Four targets that drop it here do have a column COLLATE clause; the
	// declaration is lost on the way through Ptah rather than by the server's
	// incapacity, and the record says the same thing either way, because to the
	// author the outcome is the same.
	CollateProperty = "collation"
	// UpdateExpressionProperty is the MySQL-family ON UPDATE expression. A
	// target without it leaves the column unchanged on every later write, which
	// the author declared it should not be.
	UpdateExpressionProperty = "on update expression"
	// NotNullConstraintNameProperty is the name the author gave a column's NOT
	// NULL. The PostgreSQL family answers a name it cannot persist with a
	// refusal instead, so it never records this one.
	NotNullConstraintNameProperty = "not null constraint name"
	// UniqueProperty is a column-level UNIQUE. The declaration decides which
	// rows the database accepts, so a target that drops it accepts rows the
	// author meant to exclude.
	UniqueProperty = "unique constraint"
	// AutoIncrementProperty is a column the author declared as generating its
	// own values. It is not the identity values -- those are recorded
	// separately by [Sink.RecordLostIdentity] -- but the generation itself, on
	// a target that has none to spell.
	AutoIncrementProperty = "auto-increment"
)

// ColumnProperties carries the column properties one target does not render.
//
// A zero field is a property the caller's renderer keeps, or one the column did
// not declare; both record nothing. The caller passes what its own renderer
// drops, because which properties survive is dialect knowledge and nothing
// below the renderer can answer it.
type ColumnProperties struct {
	Charset               string
	Collate               string
	UpdateExpression      string
	NotNullConstraintName string
	Unique                bool
	AutoIncrement         bool
	// AutoIncrementRemedy states how to keep a generated key on this target.
	// It is empty unless one works here, which is the rule every Remedy
	// follows: PostgreSQL spells generation two other ways and has something to
	// say, and a target with no generated column at all does not.
	AutoIncrementRemedy string
}

// RecordLostColumnProperties records the column properties a target drops.
func (s *Sink) RecordLostColumnProperties(name string, lost ColumnProperties) {
	if s == nil {
		return
	}
	for _, declared := range []struct {
		property string
		value    string
	}{
		{CharsetProperty, lost.Charset},
		{CollateProperty, lost.Collate},
		{NotNullConstraintNameProperty, lost.NotNullConstraintName},
		{UpdateExpressionProperty, lost.UpdateExpression},
	} {
		s.RecordLostProperty(ColumnKind, name, declared.property, declared.value)
	}
	// The two flags are recorded here rather than through
	// [Sink.RecordLostProperty], which reads an empty value as nothing
	// declared. That is right for a string and wrong for a flag: a boolean
	// carries no text to repeat, and passing "true" as the detail would put a
	// word in the report that the author never wrote.
	for _, flag := range []struct {
		property string
		declared bool
		remedy   string
	}{
		{AutoIncrementProperty, lost.AutoIncrement, lost.AutoIncrementRemedy},
		{UniqueProperty, lost.Unique, ""},
	} {
		if !flag.declared {
			continue
		}
		omission := PropertyOmission(ColumnKind, name, flag.property, "")
		omission.Remedy = flag.remedy
		s.Record(omission)
	}
}

// Index properties a target can decline to carry, beyond
// [ConditionProperty] and [OperatorClassProperty].
const (
	// ParserProperty is the MySQL FULLTEXT parser name, such as ngram. Only
	// the MySQL family has a clause for it.
	ParserProperty = "fulltext parser"
	// StorageParamProperty prefixes one index storage parameter. Only the
	// PostgreSQL family renders these, as WITH (key='value').
	StorageParamProperty = "index storage parameter"
	// PartOrderProperty is a descending index part. A target that reads the
	// column list and not the parts builds the index ascending, so a query
	// written for the declared order scans instead of walking the index.
	PartOrderProperty = "index part order"
	// UniqueIndexProperty is a unique index the target creates as an ordinary
	// one. The declaration decides which rows the database accepts, so losing
	// it is the index-level twin of losing a column's UNIQUE.
	UniqueIndexProperty = "unique index"
)

// RecordLostStorageParams records every index storage parameter a target drops.
//
// One record per key, in a deterministic order, because fixing some of them has
// to shorten the report rather than leave it unchanged. An index that declared
// none records nothing.
func (s *Sink) RecordLostStorageParams(index string, params map[string]string) {
	if s == nil {
		return
	}
	for _, key := range slices.Sorted(maps.Keys(params)) {
		s.Record(PropertyOmission(
			IndexKind, index, StorageParamProperty+" "+key, params[key]))
	}
}

// RecordLostUniqueIndex records a unique index the target does not enforce.
// The caller decides whether the index declared one.
//
// A renderer that answers with a SQL line comment records it here too. The
// server stores none of that comment, so the author reads "downgraded" in a
// file and the database enforces nothing, which is the same outcome as silence.
func (s *Sink) RecordLostUniqueIndex(index string) {
	s.Record(PropertyOmission(IndexKind, index, UniqueIndexProperty, ""))
}

// Kinds an omission names beside [TableKind], [ColumnKind] and [IndexKind].
const (
	// SchemaKind owns a property lost from a CREATE SCHEMA.
	SchemaKind = "schema"
	// RoleKind owns a property lost from a CREATE ROLE.
	RoleKind = "role"
	// MaterializedViewKind owns a property lost from a CREATE MATERIALIZED
	// VIEW.
	MaterializedViewKind = "materialized view"
)

// Properties a whole node can declare and a target decline to carry.
const (
	// PartitionProperty is a table's partitioning. Losing it produces one
	// ordinary table where the author declared a partitioned one, so every row
	// lands in the same place and a partition-wise plan is not available.
	PartitionProperty = "partition"
	// RefreshProperty is a materialized view's refresh schedule. A target that
	// drops it creates a view that is populated once and never again, which
	// reads as stale data rather than as a missing clause.
	RefreshProperty = "refresh schedule"
)

// RecordLostRefresh records a materialized view's refresh schedule the target
// does not carry. A view declaring none records nothing.
//
// A view created without it is populated once and never again, which reaches
// the reader as stale data rather than as a missing clause -- so the schedule is
// worth naming even though the view itself was created.
func (s *Sink) RecordLostRefresh(view string, spec *ast.MatViewRefreshSpec) {
	if s == nil || spec == nil {
		return
	}
	s.RecordLostProperty(MaterializedViewKind, view, RefreshProperty,
		strings.TrimSpace(spec.Mode+" "+spec.Interval))
}

// RecordLostPartition records a table partitioning the target does not carry.
// A table declaring none records nothing.
//
// The spec is described here rather than by each renderer. Five targets drop
// it, and five descriptions of the same declaration are five things to keep in
// step; taking the node is what makes the sentence one.
func (s *Sink) RecordLostPartition(table string, spec *ast.PartitionSpec) {
	if s == nil || spec == nil {
		return
	}
	keys := make([]string, 0, len(spec.Parts))
	for _, part := range spec.Parts {
		if part.Expr != "" {
			keys = append(keys, part.Expr)
			continue
		}
		keys = append(keys, part.Name)
	}
	s.RecordLostProperty(TableKind, table, PartitionProperty,
		fmt.Sprintf("%s (%s)", spec.Type, strings.Join(keys, ", ")))
}
