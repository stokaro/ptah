package embedydb

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/embedrun"
	"ptah.run/internal/embedstore"
)

// table is one run-state table: its name and its columns, which are
// [embedstore]'s field descriptors in their declared order.
//
// The columns are read from the descriptors rather than written out here, so a
// field added to a descriptor is a column every statement below names. Binding
// a row is by column name, and a descriptor field this file has no value for
// is an error rather than a column left out of an UPSERT.
type table struct {
	name    string
	columns []schemamodel.Field
	key     []string
}

// The run-state tables. The keys are the PostgreSQL store's.
var (
	generationTable = table{
		name: embedstore.GenerationTable, columns: embedstore.GenerationFields(),
		key: []string{"identity"},
	}
	runTable = table{
		name: embedstore.RunTable, columns: embedstore.RunFields(),
		key: []string{"id"},
	}
	eventTable = table{
		name: embedstore.EventTable, columns: embedstore.EventFields(),
		key: []string{"run_id", "sequence"},
	}
	pointerTable = table{
		name: embedstore.PointerTable, columns: embedstore.PointerFields(),
		key: []string{"target_schema", "target_table"},
	}
)

// storeTables lists them in the order they are created.
func storeTables() []table {
	return []table{generationTable, runTable, eventTable, pointerTable}
}

// selectList is the table's columns, quoted, for a SELECT.
func (t table) selectList() string {
	quoted := make([]string, len(t.columns))
	for index, column := range t.columns {
		quoted[index] = quote(column.Name)
	}
	return strings.Join(quoted, ", ")
}

// upsert renders the statement that writes one whole row, and binds values to
// it. Every column is written, so the row holds exactly what the decision
// produced.
func (t table) upsert(values map[string]any) (string, []any, error) {
	arguments := make([]any, 0, len(t.columns))
	placeholders := make([]string, 0, len(t.columns))
	for _, column := range t.columns {
		value, found := values[column.Name]
		if !found {
			return "", nil, fmt.Errorf("write %s: no value for column %s", t.name, column.Name)
		}
		arguments = append(arguments, value)
		placeholders = append(placeholders, "$p"+strconv.Itoa(len(arguments)))
	}
	if len(values) != len(t.columns) {
		return "", nil, fmt.Errorf("write %s: %d values for %d columns", t.name, len(values), len(t.columns))
	}
	statement := fmt.Sprintf("UPSERT INTO %s (%s) VALUES (%s)",
		quote(t.name), t.selectList(), strings.Join(placeholders, ", "))
	return statement, arguments, nil
}

// scanned is one row read back, by column name.
type scanned map[string]any

// scan reads one row of the table's columns into a [scanned].
func (t table) scan(row interface{ Scan(...any) error }) (scanned, error) {
	destinations := make([]any, len(t.columns))
	for index, column := range t.columns {
		switch column.Type {
		case "TEXT":
			destinations[index] = new(sql.NullString)
		case "INTEGER":
			destinations[index] = new(sql.NullInt32)
		case "BIGINT":
			destinations[index] = new(sql.NullInt64)
		case "BOOLEAN":
			destinations[index] = new(sql.NullBool)
		case "TIMESTAMPTZ":
			destinations[index] = new(sql.NullTime)
		default:
			return nil, fmt.Errorf("read %s: column %s has type %s, which this store does not read",
				t.name, column.Name, column.Type)
		}
	}
	if err := row.Scan(destinations...); err != nil {
		return nil, err
	}
	values := make(scanned, len(t.columns))
	for index, column := range t.columns {
		values[column.Name] = destinations[index]
	}
	return values, nil
}

func (s scanned) text(name string) string {
	value, _ := s[name].(*sql.NullString)
	if value == nil {
		return ""
	}
	return value.String
}

func (s scanned) integer(name string) int64 {
	switch value := s[name].(type) {
	case *sql.NullInt64:
		return value.Int64
	case *sql.NullInt32:
		return int64(value.Int32)
	}
	return 0
}

func (s scanned) boolean(name string) bool {
	value, _ := s[name].(*sql.NullBool)
	return value != nil && value.Bool
}

// instant reads a time, zero for NULL, in UTC: the driver hands a YDB
// timestamp back in the process's local zone.
func (s scanned) instant(name string) time.Time {
	value, _ := s[name].(*sql.NullTime)
	if value == nil || !value.Valid {
		return time.Time{}
	}
	return value.Time.UTC()
}

// optional binds a string that is NULL when empty, as the PostgreSQL store
// does: empty and absent are one state for every optional column.
func optional(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

// optionalInstant binds a time that is NULL when zero.
func optionalInstant(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	return new(storedInstant(value))
}

// storedInstant is a time as YDB stores it: in UTC, to the microsecond.
//
// A decision made in memory carries nanoseconds and a row read back carries
// microseconds, so every time a decision writes is truncated first. Otherwise
// the value a caller is handed and the value a later read returns differ, and
// a comparison of the two -- a rollback rechecking the maintenance deadline a
// cutover recorded -- refuses a state nothing changed.
func storedInstant(value time.Time) time.Time {
	if value.IsZero() {
		return value
	}
	return value.UTC().Truncate(time.Microsecond)
}

func generationValues(generation embedstore.Generation) map[string]any {
	return map[string]any{
		"identity":               generation.Identity,
		"spec_digest":            generation.SpecDigest,
		"spec_document":          generation.SpecDocument,
		"name":                   optional(generation.Name),
		"reproducibility":        generation.Reproducibility,
		"reproducibility_reason": optional(generation.ReproducibilityReason),
		"resolved_model":         optional(generation.ResolvedModel),
		"dimension":              int32(generation.Dimension), // #nosec G115 -- a vector dimension, bounded by the provider check
		"target_schema":          generation.TargetSchema,
		"target_table":           generation.TargetTable,
		"target_column":          generation.TargetColumn,
		"source_schema":          generation.SourceSchema,
		"source_table":           generation.SourceTable,
		"consistency_mode":       generation.ConsistencyMode,
		"created_at":             storedInstant(generation.CreatedAt),
		"retired_at":             optionalInstant(generation.RetiredAt),
		"verified_at":            optionalInstant(generation.VerifiedAt),
		"maintained_until":       optionalInstant(generation.MaintainedUntil),
	}
}

func scannedGeneration(row scanned) embedstore.Generation {
	return embedstore.Generation{
		Identity:              row.text("identity"),
		SpecDigest:            row.text("spec_digest"),
		SpecDocument:          row.text("spec_document"),
		Name:                  row.text("name"),
		Reproducibility:       row.text("reproducibility"),
		ReproducibilityReason: row.text("reproducibility_reason"),
		ResolvedModel:         row.text("resolved_model"),
		Dimension:             int(row.integer("dimension")),
		TargetSchema:          row.text("target_schema"),
		TargetTable:           row.text("target_table"),
		TargetColumn:          row.text("target_column"),
		SourceSchema:          row.text("source_schema"),
		SourceTable:           row.text("source_table"),
		ConsistencyMode:       row.text("consistency_mode"),
		CreatedAt:             row.instant("created_at"),
		RetiredAt:             row.instant("retired_at"),
		VerifiedAt:            row.instant("verified_at"),
		MaintainedUntil:       row.instant("maintained_until"),
	}
}

func runValues(run embedrun.Run) (map[string]any, error) {
	cursor, err := encodeCursor(run.Cursor)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"id":                     run.ID,
		"spec_digest":            run.SpecDigest,
		"generation_identity":    run.GenerationIdentity,
		"environment":            run.Environment,
		"source":                 run.Source,
		"target":                 run.Target,
		"provider_profile":       run.ProviderProfile,
		"resolved_model":         optional(run.ResolvedModel),
		"ptah_version":           run.PtahVersion,
		"policy_digest":          run.PolicyDigest,
		"phase":                  string(run.Phase),
		"status":                 string(run.Status),
		"lease_owner":            optional(run.LeaseOwner),
		"lease_expires":          optionalInstant(run.LeaseExpires),
		"fencing_token":          run.FencingToken,
		"snapshot_done":          run.SnapshotDone,
		"snapshot_watermark":     optional(run.SnapshotWatermark),
		"catch_up_watermark":     optional(run.CatchUpWatermark),
		"cursor":                 cursor,
		"rows_scanned":           run.Progress.RowsScanned,
		"rows_embedded":          run.Progress.RowsEmbedded,
		"rows_skipped":           run.Progress.RowsSkipped,
		"rows_deleted":           run.Progress.RowsDeleted,
		"batches_committed":      run.Progress.BatchesCommitted,
		"provider_prompt_tokens": run.Progress.ProviderPromptTokens,
		"provider_total_tokens":  run.Progress.ProviderTotalTokens,
		"provider_usage_batches": run.Progress.ProviderUsageBatches,
		"retry_count":            int32(run.Progress.RetryCount), // #nosec G115 -- retries within one batch
		"verification_ref":       optional(run.VerificationRef),
		"cutover_plan_ref":       optional(run.CutoverPlanRef),
		"approval_ref":           optional(run.ApprovalRef),
		"active_pointer":         optional(run.ActivePointer),
		"rollback_eligible":      run.RollbackEligible,
		"failure_class":          optional(run.FailureClass),
		"failure_detail":         optional(run.FailureDetail),
		"created_at":             storedInstant(run.CreatedAt),
		"updated_at":             storedInstant(run.UpdatedAt),
	}, nil
}

func scannedRun(row scanned) (embedrun.Run, error) {
	cursor, err := decodeCursor(row.text("cursor"))
	if err != nil {
		return embedrun.Run{}, err
	}
	return embedrun.Run{
		ID:                 row.text("id"),
		SpecDigest:         row.text("spec_digest"),
		GenerationIdentity: row.text("generation_identity"),
		Environment:        row.text("environment"),
		Source:             row.text("source"),
		Target:             row.text("target"),
		ProviderProfile:    row.text("provider_profile"),
		ResolvedModel:      row.text("resolved_model"),
		PtahVersion:        row.text("ptah_version"),
		PolicyDigest:       row.text("policy_digest"),
		Phase:              embedrun.Phase(row.text("phase")),
		Status:             embedrun.Status(row.text("status")),
		LeaseOwner:         row.text("lease_owner"),
		LeaseExpires:       row.instant("lease_expires"),
		FencingToken:       row.integer("fencing_token"),
		SnapshotDone:       row.boolean("snapshot_done"),
		SnapshotWatermark:  row.text("snapshot_watermark"),
		CatchUpWatermark:   row.text("catch_up_watermark"),
		Cursor:             cursor,
		Progress: embedrun.Progress{
			RowsScanned:          row.integer("rows_scanned"),
			RowsEmbedded:         row.integer("rows_embedded"),
			RowsSkipped:          row.integer("rows_skipped"),
			RowsDeleted:          row.integer("rows_deleted"),
			BatchesCommitted:     row.integer("batches_committed"),
			ProviderPromptTokens: row.integer("provider_prompt_tokens"),
			ProviderTotalTokens:  row.integer("provider_total_tokens"),
			ProviderUsageBatches: row.integer("provider_usage_batches"),
			RetryCount:           int(row.integer("retry_count")),
		},
		VerificationRef:  row.text("verification_ref"),
		CutoverPlanRef:   row.text("cutover_plan_ref"),
		ApprovalRef:      row.text("approval_ref"),
		ActivePointer:    row.text("active_pointer"),
		RollbackEligible: row.boolean("rollback_eligible"),
		FailureClass:     row.text("failure_class"),
		FailureDetail:    row.text("failure_detail"),
		CreatedAt:        row.instant("created_at"),
		UpdatedAt:        row.instant("updated_at"),
	}, nil
}

func pointerValues(pointer embedstore.Pointer) map[string]any {
	return map[string]any{
		"target_schema":       pointer.TargetSchema,
		"target_table":        pointer.TargetTable,
		"active_generation":   pointer.Active,
		"previous_generation": optional(pointer.Previous),
		"cut_over_at":         storedInstant(pointer.CutOverAt),
		"cut_over_by":         optional(pointer.CutOverBy),
		"plan_digest":         optional(pointer.PlanDigest),
	}
}

func scannedPointer(row scanned) embedstore.Pointer {
	return embedstore.Pointer{
		TargetSchema: row.text("target_schema"),
		TargetTable:  row.text("target_table"),
		Active:       row.text("active_generation"),
		Previous:     row.text("previous_generation"),
		CutOverAt:    row.instant("cut_over_at"),
		CutOverBy:    row.text("cut_over_by"),
		PlanDigest:   row.text("plan_digest"),
	}
}

func eventValues(event embedrun.Event, sequence int64) map[string]any {
	return map[string]any{
		"sequence":          sequence,
		"run_id":            event.RunID,
		"kind":              string(event.Kind),
		"at":                storedInstant(event.At),
		"actor":             optional(event.Actor),
		"fencing_token":     event.FencingToken,
		"from_phase":        optional(string(event.FromPhase)),
		"to_phase":          optional(string(event.ToPhase)),
		"detail":            optional(event.Detail),
		"rows_scanned":      event.Counts.RowsScanned,
		"rows_embedded":     event.Counts.RowsEmbedded,
		"rows_skipped":      event.Counts.RowsSkipped,
		"rows_deleted":      event.Counts.RowsDeleted,
		"batches_committed": event.Counts.BatchesCommitted,
	}
}

func scannedEvent(row scanned) embedrun.Event {
	return embedrun.Event{
		RunID:        row.text("run_id"),
		Kind:         embedrun.EventKind(row.text("kind")),
		At:           row.instant("at"),
		Actor:        row.text("actor"),
		FencingToken: row.integer("fencing_token"),
		FromPhase:    embedrun.Phase(row.text("from_phase")),
		ToPhase:      embedrun.Phase(row.text("to_phase")),
		Detail:       row.text("detail"),
		Counts: embedrun.Progress{
			RowsScanned:      row.integer("rows_scanned"),
			RowsEmbedded:     row.integer("rows_embedded"),
			RowsSkipped:      row.integer("rows_skipped"),
			RowsDeleted:      row.integer("rows_deleted"),
			BatchesCommitted: row.integer("batches_committed"),
		},
	}
}

// encodeCursor writes a keyset cursor as the PostgreSQL store does: a JSON
// array, because a key component may hold any character a separator could.
func encodeCursor(cursor []string) (*string, error) {
	if len(cursor) == 0 {
		return nil, nil
	}
	encoded, err := json.Marshal(cursor)
	if err != nil {
		return nil, fmt.Errorf("encode cursor: %w", err)
	}
	return new(string(encoded)), nil
}

// decodeCursor reads one back.
func decodeCursor(encoded string) ([]string, error) {
	if encoded == "" {
		return nil, nil
	}
	var cursor []string
	if err := json.Unmarshal([]byte(encoded), &cursor); err != nil {
		return nil, fmt.Errorf("decode cursor %q: %w", encoded, err)
	}
	return cursor, nil
}
