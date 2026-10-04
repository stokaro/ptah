package entities

// Job is a row table, so the schema holds a table beside its nodes.
//
//ptah:schema:table name="jobs" schema="app"
type Job struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
	//ptah:schema:field name="state" type="TEXT"
	State string
}

// Locks is a coordination node at the database root that takes YDB's
// defaults.
//
//ptah:schema:coordinationnode name="app_locks"
type Locks struct{}

// Limits is a coordination node in a directory, with every setting declared.
//
//ptah:schema:coordinationnode name="limits" schema="app" self_check_period="PT2S" session_grace_period="PT15S" read_consistency_mode="strict" attach_consistency_mode="relaxed" rate_limiter_counters_mode="detailed"
type Limits struct{}
