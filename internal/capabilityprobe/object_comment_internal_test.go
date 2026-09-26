package capabilityprobe

// White-box testing required: the object-comment keys are decided by
// objectCommentObservation, an unexported step between an accepted COMMENT ON
// and the verdict, and the only other way to reach it is a live server of each
// kind -- which is what the matrix runs, and what a unit run has none of.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// The read-back decides an accepted comment: one row is a comment where the
// reader looks, none is CockroachDB's answer to COMMENT ON TYPE, and a refused
// read-back decides nothing (stokaro/ptah#3627).
func TestObjectCommentObservation(t *testing.T) {
	readBack := "SELECT COUNT(*) WHERE obj_description('tcm'::regtype, 'pg_type') = 'x'"
	for _, tc := range []struct {
		name   string
		read   Attempt
		stored int64
		want   observation
	}{
		{
			name:   "the comment read back",
			read:   Attempt{Statement: readBack, Accepted: true},
			stored: 1,
			want:   decided(true),
		},
		{
			name:   "the comment accepted and not reported",
			read:   Attempt{Statement: readBack, Accepted: true},
			stored: 0,
			want:   annotated(false, "the server accepted the comment and obj_description does not report it"),
		},
		{
			name: "the read-back refused",
			read: Attempt{Statement: readBack, ServerErr: "function obj_description does not exist"},
			want: cannotDecide("the comment was accepted and the read-back %q was refused (%s)",
				readBack, "function obj_description does not exist"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)

			got := objectCommentObservation(tc.read, tc.stored)

			c.Assert(got, qt.Equals, tc.want)
		})
	}
}
