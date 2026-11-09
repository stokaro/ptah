package providerhost

// White-box testing required: the owner spelling a move rewrites is the
// protocol reference's database field, which the core value API does not
// expose; the black-box move through a provider is
// TestProviderMovesValuesToAnotherContextOfTheirRoot_HappyPath in
// integration/providercli.
import (
	"testing"

	qt "github.com/frankban/quicktest"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	protocol "ptah.run/provider-sdk/protocol/experimental"
)

func TestMovedOwner(t *testing.T) {
	from := &protocol.DatabaseContext{Database: "/local", SourceDatabase: "/local"}
	realm := &protocol.DatabaseContext{Database: "/local/ptah_dev/realm", SourceDatabase: "/local"}
	for _, test := range []struct {
		name         string
		database     string
		captured     *protocol.DatabaseContext
		wantDatabase string
	}{
		{name: "an owner of the database it leaves", database: "/local", captured: from, wantDatabase: ""},
		{name: "a relative owner", database: "", captured: from, wantDatabase: ""},
		{name: "an owner of another database", database: "/other", captured: from, wantDatabase: "/other"},
		{name: "a value bound to the context it moves to", database: "/local", captured: realm, wantDatabase: "/local"},
		{name: "an unbound value", database: "/local", captured: nil, wantDatabase: "/local"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			owner := &protocol.Reference{Engine: "ydb", Kind: "topic", Database: test.database, Schema: "app", Name: "events"}
			before := proto.CloneOf(owner)

			moved := movedOwner(owner, from, test.captured)

			want := proto.CloneOf(before)
			want.Database = test.wantDatabase
			c.Assert(moved, qt.CmpEquals(protocmp.Transform()), want)
			c.Assert(owner, qt.CmpEquals(protocmp.Transform()), before)
		})
	}
}
