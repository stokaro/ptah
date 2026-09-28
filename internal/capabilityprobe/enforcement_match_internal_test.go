package capabilityprobe

// White-box testing required: the MATCH keys are decided by
// recordedObservation, an unexported step between an accepted foreign key and
// the verdict, and the only other way to reach it is a live server of each
// kind -- which is what the matrix runs, and what a unit run has none of.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// The read-back decides an accepted MATCH type: one row is a type the server
// keeps, none is MariaDB 11.8.9's and SQLite 3.51's answer, which record NONE,
// and a refused read-back decides nothing (stokaro/ptah#3853).
func TestRecordedObservation(t *testing.T) {
	readBack := "SELECT COUNT(*) FROM pg_constraint WHERE conname = 'emf2_fk' AND confmatchtype = 'f'"
	for _, tc := range []struct {
		name   string
		read   Attempt
		stored int64
		want   observation
	}{
		{
			name:   "the type read back",
			read:   Attempt{Statement: readBack, Accepted: true},
			stored: 1,
			want:   decided(true),
		},
		{
			name:   "the type accepted and not recorded",
			read:   Attempt{Statement: readBack, Accepted: true},
			stored: 0,
			want:   annotated(false, "the server accepted the clause and does not record it"),
		},
		{
			name: "the read-back refused",
			read: Attempt{Statement: readBack, ServerErr: "relation pg_constraint does not exist"},
			want: cannotDecide("the clause was accepted and the read-back %q was refused (%s)",
				readBack, "relation pg_constraint does not exist"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)

			got := recordedObservation(tc.read, tc.stored)

			c.Assert(got, qt.Equals, tc.want)
		})
	}
}
