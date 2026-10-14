package pgdefaultprivilegeprovider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgdefaultprivilege"
)

func TestReverse_UnknownAclDiscardsSuccessfulPrefix(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForDialect("cockroachdb")
	ref := must.Must(pgdefaultprivilege.Ref(pgdefaultprivilege.Identity{Grantor: "owner", ObjectType: "TABLES", Grantee: "owner"}, semantics))
	other := must.Must(pgdefaultprivilege.Ref(pgdefaultprivilege.Identity{Grantor: "second", ObjectType: "TABLES", Grantee: "second"}, semantics))
	result, err := runtime(c).ReverseChanges(t.Context(), schemaext.ReversalRequest{Target: "cockroachdb", Identifiers: semantics, Changes: []schemaext.ChangeRecord{
		{Subject: ref, Value: &pgdefaultprivilege.Change{Before: &pgdefaultprivilege.Observed{}, Granted: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}}},
		{Subject: other, Value: &pgdefaultprivilege.Change{UnknownBefore: true, Granted: []pgdefaultprivilege.Grant{{Privilege: "ALL"}}}},
	}})
	c.Assert(err, qt.ErrorIs, schemaext.ErrIrreversible)
	c.Assert(result, qt.HasLen, 0)
}
