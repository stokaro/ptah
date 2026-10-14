package selectext_test

import (
	"context"
	"fmt"

	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/selectext"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/feature/pgdefaultprivilege/privilegeselect"
)

// ExampleSet_SelectAdditions retains a grant from a change that also revokes a right.
func ExampleSet_SelectAdditions() {
	set := must.Must(selectext.NewSet(privilegeselect.Extension()))
	ref := must.Must(pgdefaultprivilege.Ref(pgdefaultprivilege.Identity{Grantor: "owner", Schema: "app", ObjectType: "TABLES", Grantee: "reader"}, identifier.ForDialect("postgres")))
	changes, err := set.SelectAdditions(context.Background(), selectext.AdditiveRequest{Changes: []schemaext.ChangeRecord{{Subject: ref, Value: &pgdefaultprivilege.Change{
		Granted: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}, Revoked: []pgdefaultprivilege.Grant{{Privilege: "INSERT"}},
	}}}})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(changes[0].Value)
	// Output: grant SELECT
}

// ExampleSet_SubtractBaseline keeps a replay's new right within an existing ACL.
func ExampleSet_SubtractBaseline() {
	set := must.Must(selectext.NewSet(privilegeselect.Extension()))
	ref := must.Must(pgdefaultprivilege.Ref(pgdefaultprivilege.Identity{Grantor: "owner", Schema: "app", ObjectType: "TABLES", Grantee: "reader"}, identifier.ForDialect("postgres")))
	observed := func(grants ...pgdefaultprivilege.Grant) schemaext.Objects {
		return must.Must(schemaext.NewObjects(must.Must(pgdefaultprivilege.ObservedObject(ref, pgdefaultprivilege.Observed{Privileges: grants}))))
	}
	kept, err := set.SubtractBaseline(context.Background(), selectext.BaselineRequest{
		Current:       observed(pgdefaultprivilege.Grant{Privilege: "SELECT"}, pgdefaultprivilege.Grant{Privilege: "INSERT"}),
		StartingPoint: observed(pgdefaultprivilege.Grant{Privilege: "SELECT"}),
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	objects, err := kept.All()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(objects[0].Value.(*pgdefaultprivilege.Observed).Privileges[0].Privilege)
	// Output: INSERT
}
