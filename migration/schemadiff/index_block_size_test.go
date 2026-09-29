package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestCompare_IndexBlockSize(t *testing.T) {
	for _, test := range []struct {
		name, dialect, format string
		desired, live         uint64
		changes               bool
	}{
		{"MariaDB retains", "mariadb", "Dynamic", 8, 4, true},
		{"MariaDB equal", "mariadb", "Dynamic", 8, 8, false},
		{"MariaDB removes", "mariadb", "Dynamic", 0, 8, true},
		{"MySQL compressed retains", "mysql", "Compressed", 8, 4, true},
		{"MySQL compressed equal", "mysql", "Compressed", 8, 8, false},
		{"MySQL compressed removes", "mysql", "Compressed", 0, 8, true},
		{"MySQL dynamic drops", "mysql", "Dynamic", 8, 0, false},
		{"MySQL compact drops", "mysql", "Compact", 8, 0, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := optionedIndexTable("", false)
			live := liveOptionedIndexTable("", false)
			desired.Indexes[0].KeyBlockSize = test.desired
			live.Indexes[0].KeyBlockSize = test.live
			live.Tables[0].RowFormat = test.format
			diff := compareForDialect(test.dialect, desired, live)
			c.Assert(diff.HasChanges(), qt.Equals, test.changes)
			c.Assert(len(diff.IndexesAdded) > 0, qt.Equals, test.changes)
			c.Assert(len(diff.IndexesRemoved) > 0, qt.Equals, test.changes)
			c.Assert(desired.Indexes[0].KeyBlockSize, qt.Equals, test.desired)
		})
	}
}

func TestCompare_PrimaryKeyOptions(t *testing.T) {
	for _, test := range []struct {
		name, dialect, format, comment string
		size                           uint64
		changes                        bool
	}{
		{"MariaDB size", "mariadb", "Dynamic", "old", 8, true},
		{"MariaDB comment", "mariadb", "Dynamic", "new", 4, true},
		{"MariaDB synced", "mariadb", "Dynamic", "old", 4, false},
		{"MySQL compressed size", "mysql", "Compressed", "old", 8, true},
		{"MySQL drops size", "mysql", "Dynamic", "old", 8, false},
		{"MySQL comment", "mysql", "Dynamic", "new", 8, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := methodDesired("")
			live := methodCurrent(nil)
			desired.Tables[0].PrimaryKeyBlockSize = test.size
			desired.Tables[0].PrimaryKeyComment = test.comment
			live.Tables[0].RowFormat = test.format
			live.Constraints[0].KeyBlockSize = 4
			live.Constraints[0].Comment = "old"
			diff := compareForDialect(test.dialect, desired, live)
			c.Assert(diff.HasChanges(), qt.Equals, test.changes)
			c.Assert(len(diff.ConstraintsAdded) > 0, qt.Equals, test.changes)
			c.Assert(len(diff.ConstraintsRemoved) > 0, qt.Equals, test.changes)
		})
	}
}
