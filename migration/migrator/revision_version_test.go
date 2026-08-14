package migrator_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/migrator"
)

func TestMigrationRevision_RevisionVersionPreservesPublicAtlasIdentity(t *testing.T) {
	c := qt.New(t)

	c.Assert((migrator.MigrationRevision{
		Version:      42,
		AtlasVersion: "01.5",
	}).RevisionVersion(), qt.Equals, "01.5")
	c.Assert((migrator.MigrationRevision{
		Version: 42,
	}).RevisionVersion(), qt.Equals, "42")
}

func TestDirtyMigrationErrorNamesRevisionIdentity(t *testing.T) {
	c := qt.New(t)

	c.Assert((&migrator.DirtyMigrationError{Revision: migrator.MigrationRevision{
		Version:      42,
		AtlasVersion: "01.5",
	}}).Error(), qt.Contains, "migration 01.5 is dirty")
	c.Assert((&migrator.DirtyMigrationError{Revision: migrator.MigrationRevision{
		Version: 42,
	}}).Error(), qt.Contains, "migration 42 is dirty")
}

func TestMigrationRevisionJSONOmitsAbsentAtlasIdentity(t *testing.T) {
	c := qt.New(t)
	revision := migrator.MigrationRevision{Version: 42}

	encoded, err := json.Marshal(revision)
	c.Assert(err, qt.IsNil)
	c.Assert(string(encoded), qt.Not(qt.Contains), "atlas_version")
	var decoded migrator.MigrationRevision
	c.Assert(json.Unmarshal(encoded, &decoded), qt.IsNil)
	c.Assert(decoded.RevisionVersion(), qt.Equals, "42")
}

func TestMigrationRevisionJSONRoundTripsNonemptyAtlasIdentity(t *testing.T) {
	c := qt.New(t)
	revision := migrator.MigrationRevision{Version: 42, AtlasVersion: "01.5"}

	encoded, err := json.Marshal(revision)
	c.Assert(err, qt.IsNil)
	c.Assert(string(encoded), qt.Contains, `"atlas_version":"01.5"`)
	var decoded migrator.MigrationRevision
	c.Assert(json.Unmarshal(encoded, &decoded), qt.IsNil)
	c.Assert(decoded.RevisionVersion(), qt.Equals, "01.5")
}

func TestMigrationStatusJSONPreservesPresentEmptyCurrentVersionKey(t *testing.T) {
	c := qt.New(t)
	status := migrator.MigrationStatus{
		CurrentVersion:       42,
		CurrentVersionKey:    "",
		CurrentVersionKeySet: true,
	}

	encoded, err := json.Marshal(status)
	c.Assert(err, qt.IsNil)
	c.Assert(string(encoded), qt.Contains, `"current_version_key":""`)
	c.Assert(string(encoded), qt.Not(qt.Contains), "current_version_key_set")
	var decoded migrator.MigrationStatus
	c.Assert(json.Unmarshal(encoded, &decoded), qt.IsNil)
	c.Assert(decoded.CurrentVersion, qt.Equals, int64(42))
	c.Assert(decoded.CurrentVersionKey, qt.Equals, "")
	c.Assert(decoded.CurrentVersionKeySet, qt.IsTrue)
}

func TestMigrationStatusJSONOmitsAbsentCurrentVersionKey(t *testing.T) {
	c := qt.New(t)
	status := migrator.MigrationStatus{CurrentVersion: 42}

	encoded, err := json.Marshal(status)
	c.Assert(err, qt.IsNil)
	c.Assert(string(encoded), qt.Not(qt.Contains), "current_version_key")
	var decoded migrator.MigrationStatus
	c.Assert(json.Unmarshal(encoded, &decoded), qt.IsNil)
	c.Assert(decoded.CurrentVersion, qt.Equals, int64(42))
	c.Assert(decoded.CurrentVersionKey, qt.Equals, "")
	c.Assert(decoded.CurrentVersionKeySet, qt.IsFalse)
}

func TestMigrationStatusJSONIgnoresKeyWhenPresenceIsFalse(t *testing.T) {
	c := qt.New(t)
	status := migrator.MigrationStatus{
		CurrentVersion:    42,
		CurrentVersionKey: "stale",
	}

	encoded, err := json.Marshal(status)
	c.Assert(err, qt.IsNil)
	c.Assert(string(encoded), qt.Not(qt.Contains), "current_version_key")
	var decoded migrator.MigrationStatus
	c.Assert(json.Unmarshal(encoded, &decoded), qt.IsNil)
	c.Assert(decoded.CurrentVersion, qt.Equals, int64(42))
	c.Assert(decoded.CurrentVersionKey, qt.Equals, "")
	c.Assert(decoded.CurrentVersionKeySet, qt.IsFalse)
}

func TestMigrationStatusJSONRoundTripsNonemptyCurrentVersionKey(t *testing.T) {
	c := qt.New(t)
	status := migrator.MigrationStatus{
		CurrentVersion:       42,
		CurrentVersionKey:    "01.5",
		CurrentVersionKeySet: true,
	}

	encoded, err := json.Marshal(status)
	c.Assert(err, qt.IsNil)
	c.Assert(string(encoded), qt.Contains, `"current_version_key":"01.5"`)
	c.Assert(string(encoded), qt.Not(qt.Contains), "current_version_key_set")
	var decoded migrator.MigrationStatus
	c.Assert(json.Unmarshal(encoded, &decoded), qt.IsNil)
	c.Assert(decoded.CurrentVersion, qt.Equals, int64(42))
	c.Assert(decoded.CurrentVersionKey, qt.Equals, "01.5")
	c.Assert(decoded.CurrentVersionKeySet, qt.IsTrue)
}
