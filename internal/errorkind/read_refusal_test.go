package errorkind_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/errorkind"
)

// hiddenTriggers is a read refusal as a reader raises it.
func hiddenTriggers() *errorkind.ReadRefusal {
	return &errorkind.ReadRefusal{
		Missing: []errorkind.MissingPrivilege{{Database: "app", Objects: "triggers", Privilege: "TRIGGER on `app`.*"}},
		Err:     errors.New("the account cannot see everything"),
	}
}

// TestClassifyAReadOfTheTarget holds a read to the two conditions of its kind: the
// refusal, and the mark that says the database it read is the target. A reader
// raises the same refusal for a dev database and a schema source, where it says
// nothing about the target, and a failure of a read that is no refusal is not one
// of the account's privileges, however much it looks like a failure to connect.
func TestClassifyAReadOfTheTarget(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  error
		want errorkind.Kind
	}{
		{
			name: "a refusal under the mark, wrapped as a command wraps it",
			err:  fmt.Errorf("read database schema: %w", errorkind.TargetRead(fmt.Errorf("database app: %w", hiddenTriggers()))),
			want: errorkind.PermissionDenied,
		},
		{
			name: "a refusal from a database that is not the target",
			err:  fmt.Errorf("read --to database schema: %w", hiddenTriggers()),
			want: "",
		},
		{
			name: "a refusal from another database beside the mark of a connection",
			err:  errorkind.TargetConnection(hiddenTriggers()),
			want: "",
		},
		{
			name: "a connection lost during the read of the target",
			err:  errorkind.TargetRead(&net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset by peer")}),
			want: "",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			c.Assert(errorkind.Classify(test.err), qt.Equals, test.want)
		})
	}
}

func TestTargetReadKeepsTheError(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	cause := hiddenTriggers()

	marked := errorkind.TargetRead(cause)

	c.Assert(marked, qt.ErrorIs, cause.Err)
	c.Assert(marked, qt.ErrorAs, new(*errorkind.ReadRefusal))
	c.Assert(marked.Error(), qt.Equals, cause.Error())
	c.Assert(errorkind.TargetRead(nil), qt.IsNil)
}

// TestNewReportNamesWhatTheAccountLacks holds the typed fields to the same mark as
// the kind, so a document never lists privileges for a database it does not name.
func TestNewReportNamesWhatTheAccountLacks(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	marked := fmt.Errorf("read database schema: %w", errorkind.TargetRead(hiddenTriggers()))
	unmarked := fmt.Errorf("read --to database schema: %w", hiddenTriggers())

	c.Assert(errorkind.NewReport(marked), qt.DeepEquals, &errorkind.Report{
		Kind:              errorkind.PermissionDenied,
		Message:           marked.Error(),
		MissingPrivileges: hiddenTriggers().Missing,
	})
	c.Assert(errorkind.NewReport(unmarked), qt.DeepEquals, &errorkind.Report{Message: unmarked.Error()})
}

// TestReportWireNames pins the field names a consumer parses. A Go rename that
// changed one would pass every test that reads the report back through the same
// type.
func TestReportWireNames(t *testing.T) {
	t.Parallel()
	c := qt.New(t)

	got, err := json.Marshal(errorkind.Report{
		Kind:    errorkind.PermissionDenied,
		Message: "m",
		MissingPrivileges: []errorkind.MissingPrivilege{
			{Database: "app", Objects: "views", Names: []string{"v"}, Privilege: "SHOW VIEW"},
		},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(string(got), qt.Equals, `{"kind":"permission_denied","message":"m",`+
		`"missing_privileges":[{"database":"app","objects":"views","names":["v"],"privilege":"SHOW VIEW"}]}`)
}
