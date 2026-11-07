package providerhost

// White-box testing required: the level of a view's check option is set in
// the adapter's private mapping of AST nodes, of the schema model and of a SQL
// schema reading onto the wire. The fixture provider drives one shape through
// the CLI; the refusals and each level are only observable here.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/testing/protocmp"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
)

// checkOptionTarget is commonObjectTarget with view check options listed by
// the provider and its engine.
func checkOptionTarget() *target {
	target := commonObjectTarget(everyCommonKind...)
	target.engine.Features = append(target.engine.Features, provider.ViewCheckOptionsFeature)
	target.host.description.Features = append(target.host.description.Features, provider.ViewCheckOptionsFeature)
	return target
}

// createdView is the view a creation sends, with the level given.
func createdView(withCheck bool, option protocol.SQLView_CheckOption) *protocol.SQLView {
	return &protocol.SQLView{Name: "recent", Body: "SELECT 1", WithCheck: withCheck, CheckOption: option}
}

func TestViewCheckOptionCreation_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name   string
		target *target
		node   *ast.CreateViewNode
		want   *protocol.SQLView
	}{
		{"a LOCAL check option to an engine that lists levels", checkOptionTarget(),
			&ast.CreateViewNode{Name: "recent", Body: "SELECT 1", WithCheck: true, LocalCheck: true}, createdView(true, protocol.SQLView_LOCAL)},
		{"a check option to an engine that lists levels", checkOptionTarget(),
			&ast.CreateViewNode{Name: "recent", Body: "SELECT 1", WithCheck: true}, createdView(true, protocol.SQLView_CASCADED)},
		{"no check option to an engine that lists levels", checkOptionTarget(),
			&ast.CreateViewNode{Name: "recent", Body: "SELECT 1"}, createdView(false, protocol.SQLView_CHECK_OPTION_NONE)},
		{"a check option to an engine that does not list levels", commonObjectTarget(everyCommonKind...),
			&ast.CreateViewNode{Name: "recent", Body: "SELECT 1", WithCheck: true}, createdView(true, protocol.SQLView_CHECK_OPTION_NONE)},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			change, common, err := test.target.commonObjectChange(test.node)

			c.Assert(err, qt.IsNil)
			c.Assert(common, qt.IsTrue)
			c.Assert(change.GetCommonObject().GetCreateView().GetView(), qt.CmpEquals(protocmp.Transform()), test.want)
		})
	}
}

// An engine that does not list levels cannot be asked for LOCAL, so a LOCAL
// creation is refused by name rather than sent as CASCADED.
func TestViewCheckOptionCreation_FailurePath(t *testing.T) {
	c := qt.New(t)

	change, _, err := commonObjectTarget(everyCommonKind...).commonObjectChange(&ast.CreateViewNode{Name: "recent", Body: "SELECT 1", WithCheck: true, LocalCheck: true})

	c.Assert(err, qt.ErrorMatches, `.*WITH LOCAL CHECK OPTION of view "recent"`)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(change, qt.IsNil)
}

// A declared view carries CASCADED for a check option to an engine that lists
// levels, since the host model holds no LOCAL, and the level of no check
// option otherwise.
func TestViewCheckOptionDeclaration_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name   string
		target *target
		view   schemamodel.View
		want   protocol.SQLView_CheckOption
	}{
		{"a check option to an engine that lists levels", checkOptionTarget(), schemamodel.View{Name: "recent", Body: "SELECT 1", WithCheck: true}, protocol.SQLView_CASCADED},
		{"no check option to an engine that lists levels", checkOptionTarget(), schemamodel.View{Name: "recent", Body: "SELECT 1"}, protocol.SQLView_CHECK_OPTION_NONE},
		{"a check option to an engine that does not list levels", commonObjectTarget(everyCommonKind...),
			schemamodel.View{Name: "recent", Body: "SELECT 1", WithCheck: true}, protocol.SQLView_CHECK_OPTION_NONE},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			declared, err := test.target.declaredCommonObjects(&schemamodel.Database{Views: []schemamodel.View{test.view}})

			c.Assert(err, qt.IsNil)
			c.Assert(declared.GetViews()[0].GetCheckOption(), qt.Equals, test.want)
			c.Assert(declared.GetViews()[0].GetWithCheck(), qt.Equals, test.view.WithCheck)
		})
	}
}

// A SQL schema reading keeps a view with a CASCADED check option as one with a
// check option, and refuses one with LOCAL by name: the host model holds no
// level, and keeping it would make the check stricter.
func TestViewCheckOptionReading(t *testing.T) {
	t.Run("a CASCADED check option", func(t *testing.T) {
		c := qt.New(t)
		reply := fullSQLSchemaReply()
		reply.Views[0].WithCheck, reply.Views[0].CheckOption = true, protocol.SQLView_CASCADED

		database, err := sqlSchemaCommon(reply)

		c.Assert(err, qt.IsNil)
		c.Assert(database.Views[0].WithCheck, qt.IsTrue)
	})
	t.Run("a LOCAL check option", func(t *testing.T) {
		c := qt.New(t)
		reply := fullSQLSchemaReply()
		reply.Views[0].WithCheck, reply.Views[0].CheckOption = true, protocol.SQLView_LOCAL

		database, err := sqlSchemaCommon(reply)

		c.Assert(err, qt.ErrorMatches, `.*view "app\.visible" of the SQL schema reading has WITH LOCAL CHECK OPTION, and Ptah's view model holds no check option level`)
		c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		c.Assert(database, qt.DeepEquals, schemamodel.Database{})
	})
}

// The host lists the feature, so a provider that offers it keeps it in the
// negotiated description and receives each view's level.
func TestHostListsViewCheckOptions(t *testing.T) {
	c := qt.New(t)
	c.Assert(hostFeatures, qt.Contains, provider.ViewCheckOptionsFeature)
}
