package datadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/datadiff"
)

// A declaration writes a binary column's value as text, and a driver hands the
// same column back as []byte. These drive Compute, which is where the two meet:
// the same bytes plan nothing, and different bytes plan an update.

func payloadDiff(c *qt.C, desired, live any) *datadiff.DataDiff {
	c.Helper()
	diff, err := datadiff.Compute("", "payloads", []string{"code"},
		[]datadiff.Row{{"code": "one", "payload": desired}},
		[]datadiff.Row{{"code": "one", "payload": live}},
	)
	c.Assert(err, qt.IsNil)
	return diff
}

func TestCompute_BytesAndTextPairing_HappyPath(t *testing.T) {
	t.Run("text and the bytes it spells are one value", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(payloadDiff(c, `A\B`, []byte{0x41, 0x5c, 0x42}).Updates, qt.HasLen, 0)
	})

	t.Run("a binary key finds the row its text key names", func(t *testing.T) {
		c := qt.New(t)
		diff, err := datadiff.Compute("", "payloads", []string{"payload"},
			[]datadiff.Row{{"payload": "k1", "label": "one"}},
			[]datadiff.Row{{"payload": []byte("k1"), "label": "one"}},
		)
		c.Assert(err, qt.IsNil)
		c.Assert(diff.Inserts, qt.HasLen, 0)
		c.Assert(diff.Updates, qt.HasLen, 0)
		c.Assert(diff.Deletes, qt.HasLen, 0)
	})
}

func TestCompute_BytesAndTextPairing_FailurePath(t *testing.T) {
	t.Run("different bytes are an update", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(payloadDiff(c, `A\B`, []byte{0x41, 0x5c, 0x5c, 0x42}).Updates, qt.HasLen, 1)
	})

	t.Run("empty bytes are not NULL", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(payloadDiff(c, nil, make([]byte, 0)).Updates, qt.HasLen, 1)
	})
}
