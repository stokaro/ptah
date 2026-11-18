package publicationreverse_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpublication"
	"ptah.run/feature/pgpublication/publicationreverse"
)

var (
	orders = pgpublication.Member{Schema: "public", Table: "orders"}
	items  = pgpublication.Member{Schema: "public", Table: "items"}
	held   = &pgpublication.ObservedPublication{Tables: []pgpublication.Member{orders}, Publish: pgpublication.PublishOperations(), Owner: "app"}
)

func reverse(c *qt.C, change *pgpublication.Change) (schemaext.Reversal, error) {
	c.Helper()
	reversed, err := publicationreverse.Service{}.ReverseChanges(c.Context(), schemaext.ReversalRequest{Target: platform.Postgres,
		Changes: []schemaext.ChangeRecord{{Subject: pgpublication.Ref("feed"), Value: change}}})
	if err != nil {
		return schemaext.Reversal{}, err
	}
	c.Assert(reversed, qt.HasLen, 1)
	return reversed[0], nil
}

// TestReverseChanges_HappyPath pins the reverse of each change: a creation is
// dropped, a drop is created again with the owner it had, and a survivor goes
// back member by member and to the owner it had. A rollback that drops the
// publication says what its subscribers miss meanwhile.
func TestReverseChanges_HappyPath(t *testing.T) {
	limitation := []string{"A subscription that reads publication feed fails while the publication is dropped, and the changes made " +
		"meanwhile are not published to it when the publication is created again."}
	tests := []struct {
		name        string
		forward     *pgpublication.Change
		want        *pgpublication.Change
		limitations []string
	}{
		{name: "a creation", forward: &pgpublication.Change{After: &pgpublication.DesiredPublication{Tables: []pgpublication.Member{orders}}},
			want: &pgpublication.Change{Before: &pgpublication.ObservedPublication{Tables: []pgpublication.Member{orders},
				Publish: pgpublication.PublishOperations()}}},
		{name: "a drop", forward: &pgpublication.Change{Before: held},
			want:        &pgpublication.Change{After: &pgpublication.DesiredPublication{Tables: []pgpublication.Member{orders}, Owner: "app"}},
			limitations: limitation},
		{name: "a member added", forward: &pgpublication.Change{Before: held,
			After: &pgpublication.DesiredPublication{Tables: []pgpublication.Member{orders, items}}, AddTables: []pgpublication.Member{items}},
			want: &pgpublication.Change{
				Before: &pgpublication.ObservedPublication{Tables: []pgpublication.Member{items, orders}, Publish: pgpublication.PublishOperations(), Owner: "app"},
				After:  &pgpublication.DesiredPublication{Tables: []pgpublication.Member{orders}}, DropTables: []pgpublication.Member{items}}},
		{name: "an owner given away", forward: &pgpublication.Change{Before: held,
			After: &pgpublication.DesiredPublication{Tables: []pgpublication.Member{orders}, Owner: "replicator"}, Owner: "replicator"},
			want: &pgpublication.Change{
				Before: &pgpublication.ObservedPublication{Tables: []pgpublication.Member{orders}, Publish: pgpublication.PublishOperations(), Owner: "replicator"},
				After:  &pgpublication.DesiredPublication{Tables: []pgpublication.Member{orders}, Owner: "app"}, Owner: "app"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			reversed, err := reverse(c, test.forward)

			c.Assert(err, qt.IsNil)
			c.Assert(reversed.Change.Value, qt.DeepEquals, test.want)
			c.Assert(reversed.Limitations, qt.DeepEquals, test.limitations)
		})
	}
}

// TestReverseChanges_FailurePath pins the requests refused: another target,
// and a change no comparison makes.
func TestReverseChanges_FailurePath(t *testing.T) {
	c := qt.New(t)

	reversed, err := publicationreverse.Service{}.ReverseChanges(c.Context(), schemaext.ReversalRequest{Target: platform.YugabyteDB,
		Changes: []schemaext.ChangeRecord{{Subject: pgpublication.Ref("feed"), Value: &pgpublication.Change{Before: held}}}})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
	c.Assert(reversed, qt.IsNil)

	invalid, err := reverse(c, &pgpublication.Change{})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(invalid, qt.DeepEquals, schemaext.Reversal{})
}
