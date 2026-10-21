package providerhost

// White-box testing required: wire conversion and negotiated handle selection
// must be checked without starting a provider or trusting a remote receipt.

import (
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/core/ptaherr"
	"ptah.run/dbschema"
)

func TestMetadataStoredFactsRoundTrip(t *testing.T) {
	c := qt.New(t)
	log := dbschema.MigrationLogRecord{RunID: "run", Seq: 7, Operation: "down", Version: -3, State: "failed", Actor: "release-bot", ActorSource: "provided", Checksum: "checksum", At: time.Unix(-1, 123456789).UTC(), Error: "failure\x00detail"}
	wire, err := metadataWireLog(log)
	c.Assert(err, qt.IsNil)
	c.Assert(metadataLog(wire), qt.DeepEquals, log)
	tag := dbschema.MigrationTagRecord{Name: "release", Version: 0, RecordedAt: time.Unix(1790000000, 987654321).UTC()}
	encoded, err := metadataWireTag(tag)
	c.Assert(err, qt.IsNil)
	c.Assert(metadataTag(encoded), qt.DeepEquals, tag)
}
func TestMetadataRefusesUnknownFactsBeforeDispatch(t *testing.T) {
	c := qt.New(t)
	_, err := metadataWireLog(dbschema.MigrationLogRecord{RunID: "run", Seq: 1, Operation: "up", State: "invented", ActorSource: "unknown", At: time.Now().UTC()})
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	_, err = metadataWireTag(dbschema.MigrationTagRecord{Name: "release"})
	c.Assert(err, qt.IsNotNil)
}
func TestMetadataRequiresServiceOnEngineAndProcess(t *testing.T) {
	c := qt.New(t)
	owner := &session{target: &target{engine: &protocol.Engine{}, host: &Host{description: &protocol.Description{Features: []string{provider.MetadataFeature}}}}, remote: &protocol.Session{Id: "session"}}
	_, err := owner.MigrationLogStore()
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	owner.target.engine.Features = []string{provider.MetadataFeature}
	owner.target.host.description.Features = nil
	_, err = owner.MigrationTagsStore()
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	owner.target.host.description.Features = []string{provider.MetadataFeature}
	tx := &transaction{session: owner, id: "transaction"}
	tags, err := tx.MigrationTagsStore()
	c.Assert(err, qt.IsNil)
	scope, err := tags.(migrationTagsStore).scope(dbschema.MigrationMetadataTable{Schema: "main", Name: "tags"})
	c.Assert(err, qt.IsNil)
	c.Assert(scope.TransactionId, qt.Equals, "transaction")
	logs, err := owner.MigrationLogStore()
	c.Assert(err, qt.IsNil)
	logScope, err := logs.(migrationLogStore).scope(dbschema.MigrationMetadataTable{Schema: "main", Name: "logs"})
	c.Assert(err, qt.IsNil)
	c.Assert(logScope.TransactionId, qt.Equals, "")
	owner.SetDryRun(true)
	c.Assert(tags.Prepare(c.Context(), dbschema.MigrationMetadataTable{Name: "tags"}), qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
}
