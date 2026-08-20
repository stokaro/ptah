package migrator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/migrator"
)

// A Flyway repeatable exists to be re-run when it changes, and importing one
// makes it a one-time migration. Editing it afterwards is the ordinary Flyway
// lifecycle meeting a rule the conversion introduced, and the plain checksum
// sentence reads as tampering (stokaro/ptah#1702).

func TestChecksumMismatchError_NamesTheConversionForARepeatable(t *testing.T) {
	c := qt.New(t)

	err := &migrator.ChecksumMismatchError{
		Version:             migrator.ConvertedFlywayRepeatableVersion,
		Stored:              "aaa",
		Computed:            "bbb",
		Description:         "view",
		ConvertedRepeatable: true,
	}

	c.Assert(err.Error(), qt.Contains, `"view" was a Flyway repeatable`)
	c.Assert(err.Error(), qt.Contains, "made it a one-time migration")
	c.Assert(err.Error(), qt.Contains, "Add a new versioned migration")
}

// TestChecksumMismatchError_KeepsThePlainSentenceForAVersionedMigration is the
// control the issue asks for: the repeatable case must not be satisfied by
// softening the message for everything.
func TestChecksumMismatchError_KeepsThePlainSentenceForAVersionedMigration(t *testing.T) {
	c := qt.New(t)

	err := &migrator.ChecksumMismatchError{
		Version:     20260101000000,
		Stored:      "aaa",
		Computed:    "bbb",
		Description: "init",
	}

	c.Assert(err.Error(), qt.Equals, "migration 20260101000000 checksum mismatch: stored aaa, current bbb")
	c.Assert(err.Error(), qt.Not(qt.Contains), "repeatable")
}

// TestChecksumMismatchError_TheTwoMessagesAreNotInterchangeable is the point of
// the split: one says what happened to the file, the other says the file
// changed, and a reader acts differently on each.
func TestChecksumMismatchError_TheTwoMessagesAreNotInterchangeable(t *testing.T) {
	c := qt.New(t)
	shared := migrator.ChecksumMismatchError{Stored: "aaa", Computed: "bbb", Description: "x"}

	repeatable := shared
	repeatable.Version = migrator.ConvertedFlywayRepeatableVersion
	repeatable.ConvertedRepeatable = true
	versioned := shared
	versioned.Version = 20260101000000

	c.Assert(repeatable.Error(), qt.Not(qt.Equals), versioned.Error())
	c.Assert(versioned.Error(), qt.Not(qt.Contains), "re-import")
}
