package descriptor

// Lint states how migration lint reads a dialect's migrations.
// internal/lintdialect, internal/lintscan and migration/lint read it and
// check the enumerated values when they do. The zero value is a dialect lint
// does not analyze.
type Lint struct {
	// Analyzed reports that lint has rules and a scanner mode for the
	// dialect, so a run may name it. A dialect Ptah knows and lint cannot
	// analyze leaves it false: a run against it would match no dialect rule,
	// scan with the hybrid mode and exit 0 having read nothing it understands.
	Analyzed bool `json:"analyzed,omitempty"`
	// Scanner is the mode lint scans the dialect's text in: "hash" ('#'
	// comments, backslash escapes and executable comments, as MySQL writes
	// them), "dollar" (dollar quotes and nested comments, as PostgreSQL
	// writes them), "yql", or empty for the hybrid mode.
	Scanner string `json:"scanner,omitempty"`
	// DynamicSQL is how the dialect's routine language runs text as a
	// statement, which the injection rule reads: "execute" (EXECUTE
	// expression), "prepare-from" (PREPARE name FROM expression), "exec"
	// (EXEC (expression) and sp_executesql), or empty for none lint reads.
	DynamicSQL string `json:"dynamic_sql,omitempty"`
	// InjectionAdvice is the safe way to build a statement in that language,
	// which an injection finding ends with. Empty gives the generic advice.
	InjectionAdvice string `json:"injection_advice,omitempty"`
	// NotNullConsequence says what the engine does with a row holding NULL
	// when a column is made NOT NULL by restating its definition, which the
	// rule for that change reports. Empty means the rule does not run on the
	// dialect.
	NotNullConsequence string `json:"not_null_consequence,omitempty"`
	// NotNullByType reports that a column is made NOT NULL by a type that
	// drops its Nullable wrapper rather than by a NOT NULL clause.
	NotNullByType bool `json:"not_null_by_type,omitempty"`
	// SetNotNullRebuilds reports that the engine enforces NOT NULL on
	// existing nullable data only through a staged rebuild and backfill.
	SetNotNullRebuilds bool `json:"set_not_null_rebuilds,omitempty"`
	// OnlineMeasured reports that the online lint mode has a measurement for
	// the dialect behind it.
	OnlineMeasured bool `json:"online_measured,omitempty"`
	// AtlasAnalyzer is the code prefix of the Atlas lint analyzer whose
	// engine-specific codes cover the dialect, such as "MY", so an Atlas code
	// a policy names is aliased only where its analyzer runs. Empty means no
	// engine-specific Atlas analyzer covers it.
	AtlasAnalyzer string `json:"atlas_analyzer,omitempty"`
	// TypeRestatements is the type grammar that proves a type clause
	// restates a column's current type, named for the package that reads it:
	// "pgtype" or "mytype" under internal/lintkit, or empty for none.
	TypeRestatements string `json:"type_restatements,omitempty"`
}
