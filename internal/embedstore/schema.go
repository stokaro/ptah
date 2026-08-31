// Package embedstore is where an inference migration's own state lives, and
// what may move it.
//
// The state lives in the TARGET database rather than in a file or a control
// plane, and that is the only choice that makes the epic's checkpoint rule
// possible: a checkpoint must not advance beyond target effects that are
// durably committed, and the only way to guarantee that is for the checkpoint
// and the target write to be one transaction. A store somewhere else can
// promise reconciliation; it cannot promise this (stokaro/ptah#2068).
package embedstore

import (
	"go.5x5.cz/ptah/core/schemamodel"
)

// TablePrefix is what every table this package owns is named with.
//
// A prefix rather than a schema, because a generation's target table may live
// anywhere and a run has to be creatable beside it without asking for the
// privilege to create schemas.
const TablePrefix = "ptah_embedding_"

// Table names.
const (
	// GenerationTable is the registry of generations Ptah knows about.
	GenerationTable = TablePrefix + "generation"
	// RunTable is the durable state of a run.
	RunTable = TablePrefix + "run"
	// EventTable is the audit trail.
	EventTable = TablePrefix + "event"
	// PointerTable records which generation queries read.
	PointerTable = TablePrefix + "pointer"
)

// Objects is the schema this package needs in order to record anything.
//
// It is described in Ptah's own vocabulary rather than written as DDL, for the
// same reason a generation's target column is: Ptah already renders, compares
// and plans schema objects for every target it supports, and a second DDL path
// here would be a second answer to what these tables are.
func Objects() []schemamodel.Table {
	return []schemamodel.Table{
		generationTable(),
		runTable(),
		eventTable(),
		pointerTable(),
	}
}

// Indexes is the indexes over those tables.
func Indexes() []schemamodel.Index {
	return []schemamodel.Index{
		{
			StructName: RunTable, Name: RunTable + "_generation_idx",
			Fields:  []string{"generation_identity"},
			Comment: "a run is almost always looked up by the generation it builds",
		},
		{
			StructName: EventTable, Name: EventTable + "_run_at_idx",
			Fields:  []string{"run_id", "at"},
			Comment: "the audit trail is read as one run's history, in order",
		},
	}
}

// generationTable is the registry.
func generationTable() schemamodel.Table {
	return schemamodel.Table{
		StructName: GenerationTable, Name: GenerationTable,
		Comment: "embedding generations Ptah has been asked to build",
	}
}

// GenerationFields are the registry's columns.
func GenerationFields() []schemamodel.Field {
	return []schemamodel.Field{
		text(GenerationTable, "identity", false, "the generation's content address"),
		text(GenerationTable, "spec_digest", false, "the specification it was derived from"),
		text(GenerationTable, "name", true, "a display name, which is outside the identity"),
		text(GenerationTable, "reproducibility", false, "full or partial"),
		text(GenerationTable, "reproducibility_reason", true, "what is unpinned when it is partial"),
		text(GenerationTable, "resolved_model", true, "the model identity the provider reported"),
		integer(GenerationTable, "dimension", false, "the vector dimension"),
		text(GenerationTable, "target_schema", false, "the schema that table is in, empty for search_path"),
		text(GenerationTable, "target_table", false, "where its vectors live"),
		text(GenerationTable, "target_column", false, "and in which column"),
		timestamp(GenerationTable, "created_at", false, "when Ptah first recorded it"),
		timestamp(GenerationTable, "retired_at", true, "when it was destroyed, which is terminal"),
		timestamp(GenerationTable, "verified_at", true,
			"when a verification last passed over it, which is what a rollback rests on"),
		timestamp(GenerationTable, "maintained_until", true,
			"how long something is keeping it current, which is what makes it a way back"),
	}
}

// runTable is the durable run state.
func runTable() schemamodel.Table {
	return schemamodel.Table{
		StructName: RunTable, Name: RunTable,
		Comment: "the state a run resumes from after a restart",
	}
}

// RunFields are the run's columns.
//
// The list is written out rather than reflected so that persisting a new field
// is a decision. TestRunFields_CoverEveryRunField enumerates embedrun.Run and
// requires each leaf field to appear here or in runFieldsNotPersisted with the
// reason -- because a field added to Run and not to this list is a field a
// resumed run silently forgets, and nothing about that failure looks like a
// storage bug.
func RunFields() []schemamodel.Field {
	return []schemamodel.Field{
		text(RunTable, "id", false, "the run's identifier"),
		text(RunTable, "spec_digest", false, "the specification"),
		text(RunTable, "generation_identity", false, "the generation being built"),
		text(RunTable, "environment", false, "where the run operates"),
		text(RunTable, "source", false, "what it reads"),
		text(RunTable, "target", false, "what it writes"),
		text(RunTable, "provider_profile", false, "through which provider"),
		text(RunTable, "resolved_model", true, "the model identity the provider reported"),
		text(RunTable, "ptah_version", false, "what produced the run"),
		text(RunTable, "policy_digest", false, "what governed it"),
		text(RunTable, "phase", false, "where the run is"),
		text(RunTable, "status", false, "whether it is moving"),
		text(RunTable, "lease_owner", true, "the worker that holds it"),
		timestamp(RunTable, "lease_expires", true, "and until when"),
		bigint(RunTable, "fencing_token", false, "what makes the lease enforceable"),
		text(RunTable, "snapshot_watermark", true, "the boundary the backfill covers up to"),
		text(RunTable, "catch_up_watermark", true, "how far catch-up has processed past it"),
		text(RunTable, "cursor", true, "the keyset position the backfill resumes from"),
		bigint(RunTable, "rows_scanned", false, "source rows read"),
		bigint(RunTable, "rows_embedded", false, "vectors written"),
		bigint(RunTable, "rows_skipped", false, "rows the specification declined"),
		bigint(RunTable, "rows_deleted", false, "rows tombstoned"),
		bigint(RunTable, "batches_committed", false, "checkpoints behind the cursor"),
		bigint(RunTable, "provider_prompt_tokens", false, "what the provider reported"),
		bigint(RunTable, "provider_total_tokens", false, "and in total"),
		integer(RunTable, "retry_count", false, "retries since the last checkpoint"),
		text(RunTable, "verification_ref", true, "the verification report"),
		text(RunTable, "cutover_plan_ref", true, "the cutover plan"),
		text(RunTable, "approval_ref", true, "the approval bound to it"),
		text(RunTable, "active_pointer", true, "the generation queries read"),
		boolean(RunTable, "rollback_eligible", false, "whether the previous one is still a way back"),
		text(RunTable, "failure_class", true, "why a failed run stopped"),
		text(RunTable, "failure_detail", true, "and what happened"),
		timestamp(RunTable, "created_at", false, "when the run began"),
		timestamp(RunTable, "updated_at", false, "when it last moved"),
	}
}

// eventTable is the audit trail.
func eventTable() schemamodel.Table {
	return schemamodel.Table{
		StructName: EventTable, Name: EventTable,
		Comment: "what happened to a run, in order, and never what it embedded",
	}
}

// EventFields are the audit trail's columns.
//
// There is no column for source text and none for a vector, and that is
// enforced rather than remembered: TestEventFields_HoldNoContent requires it,
// as embedrun's own reflection test does for the struct.
func EventFields() []schemamodel.Field {
	return []schemamodel.Field{
		bigint(EventTable, "sequence", false, "the event's position in the run's history"),
		text(EventTable, "run_id", false, "the run"),
		text(EventTable, "kind", false, "what happened"),
		timestamp(EventTable, "at", false, "when"),
		text(EventTable, "actor", true, "who"),
		bigint(EventTable, "fencing_token", false, "the token they held"),
		text(EventTable, "from_phase", true, "the phase moved from"),
		text(EventTable, "to_phase", true, "and to"),
		text(EventTable, "detail", true, "prose about the run, never row content"),
		bigint(EventTable, "rows_scanned", false, "the run's counts at that moment"),
		bigint(EventTable, "rows_embedded", false, ""),
		bigint(EventTable, "rows_skipped", false, ""),
		bigint(EventTable, "rows_deleted", false, ""),
		bigint(EventTable, "batches_committed", false, ""),
	}
}

// pointerTable records which generation queries read.
func pointerTable() schemamodel.Table {
	return schemamodel.Table{
		StructName: PointerTable, Name: PointerTable,
		Comment: "which generation each target column's queries read",
	}
}

// PointerFields are the pointer's columns.
func PointerFields() []schemamodel.Field {
	return []schemamodel.Field{
		text(PointerTable, "target_schema", false, "the schema that table is in, empty for search_path"),
		text(PointerTable, "target_table", false, "the table the pointer is about"),
		text(PointerTable, "active_generation", false, "what queries read now"),
		text(PointerTable, "previous_generation", true, "what they read before"),
		timestamp(PointerTable, "cut_over_at", false, "when it moved"),
		text(PointerTable, "cut_over_by", true, "who moved it"),
		text(PointerTable, "plan_digest", true, "the plan that authorized the move"),
	}
}

// text is a string column.
func text(table, name string, nullable bool, comment string) schemamodel.Field {
	return schemamodel.Field{StructName: table, Name: name, Type: "TEXT", Nullable: nullable, Comment: comment}
}

// integer is a 32-bit integer column.
func integer(table, name string, nullable bool, comment string) schemamodel.Field {
	return schemamodel.Field{StructName: table, Name: name, Type: "INTEGER", Nullable: nullable, Comment: comment}
}

// bigint is a 64-bit integer column.
func bigint(table, name string, nullable bool, comment string) schemamodel.Field {
	return schemamodel.Field{StructName: table, Name: name, Type: "BIGINT", Nullable: nullable, Comment: comment}
}

// boolean is a boolean column.
func boolean(table, name string, nullable bool, comment string) schemamodel.Field {
	return schemamodel.Field{StructName: table, Name: name, Type: "BOOLEAN", Nullable: nullable, Comment: comment}
}

// timestamp is a point in time.
func timestamp(table, name string, nullable bool, comment string) schemamodel.Field {
	return schemamodel.Field{
		StructName: table, Name: name, Type: "TIMESTAMPTZ", Nullable: nullable, Comment: comment,
	}
}

// runFieldsNotPersisted names every embedrun.Run field deliberately outside the
// run table, with the reason.
//
// Each entry is a promise that a run resumed without it resumes correctly.
var runFieldsNotPersisted = make(map[string]string)
