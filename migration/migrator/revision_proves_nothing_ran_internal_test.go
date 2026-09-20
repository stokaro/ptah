package migrator

// White-box testing required: revisionProvesNothingRan joins a row reading with
// a dialect class, and the classes that make the two disagree are ClickHouse,
// Oracle and Spanner, which no in-process test can connect to. Driving the
// predicate directly is what covers them without a server.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// A dirty revision with applied=0 means "nothing is in the database" only where
// a failed body rolls back with the write that records it. Where each statement
// commits on its own there is no checkpoint to write, so the same row is what a
// committed body leaves when the revision write fails, and the repair that
// records it applied is the documented recovery (stokaro/ptah#3452, #999).
func TestRevisionProvesNothingRan(t *testing.T) {
	zeroProgress := &MigrationRevision{
		Version:   1,
		Dirty:     true,
		Direction: MigrationDirectionUp,
		Applied:   0,
		Total:     2,
	}

	tests := []struct {
		name     string
		dialect  string
		revision *MigrationRevision
		want     bool
	}{
		{name: "postgres rolls the body back", dialect: "postgres", revision: zeroProgress, want: true},
		{name: "sqlite rolls the body back", dialect: "sqlite", revision: zeroProgress, want: true},
		{name: "clickhouse commits each statement", dialect: "clickhouse", revision: zeroProgress, want: false},
		{name: "oracle commits each statement", dialect: "oracle", revision: zeroProgress, want: false},
		{name: "spanner commits each statement", dialect: "spanner", revision: zeroProgress, want: false},
		{name: "mysql commits before ddl", dialect: "mysql", revision: zeroProgress, want: false},
		{
			name:     "an unclassified dialect is read as rolling back",
			dialect:  "",
			revision: zeroProgress,
			want:     true,
		},
		{
			name:    "a statement in flight is not proof",
			dialect: "postgres",
			revision: &MigrationRevision{
				Version:   1,
				Dirty:     true,
				Direction: MigrationDirectionUp,
				Applied:   0,
				Total:     2,
				Error:     unknownStatementOutcomeError,
			},
			want: false,
		},
		{
			name:    "committed progress is not proof",
			dialect: "postgres",
			revision: &MigrationRevision{
				Version:   1,
				Dirty:     true,
				Direction: MigrationDirectionUp,
				Applied:   1,
				Total:     2,
			},
			want: false,
		},
		{
			name:    "a clean row is not proof",
			dialect: "postgres",
			revision: &MigrationRevision{
				Version:   1,
				Direction: MigrationDirectionUp,
				Applied:   0,
				Total:     2,
			},
			want: false,
		},
		{name: "no row is not proof", dialect: "postgres", revision: nil, want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(revisionProvesNothingRan(test.revision, test.dialect), qt.Equals, test.want)
		})
	}
}
