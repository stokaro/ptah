package compare

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/internal/exitcode"
	difftypes "github.com/stokaro/ptah/migration/schemadiff/types"
)

func TestCompareExitCode(t *testing.T) {
	tests := []struct {
		name     string
		diff     *difftypes.SchemaDiff
		wantCode int
	}{
		{name: "empty diff", diff: &difftypes.SchemaDiff{}, wantCode: 0},
		{name: "non-empty diff", diff: &difftypes.SchemaDiff{TablesAdded: []string{"users"}}, wantCode: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			err := nonEmptyDiffExitCode(tt.diff)

			if tt.wantCode == 0 {
				c.Assert(err, qt.IsNil)
				return
			}
			c.Assert(err, qt.IsNotNil)
			c.Assert(exitcode.Code(err, 0), qt.Equals, tt.wantCode)
		})
	}
}
