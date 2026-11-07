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

// A declared view carries the level the model holds to an engine that lists
// levels, and the level of no check option, beside with_check, to one that
// does not.
func TestViewCheckOptionDeclaration_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name   string
		target *target
		view   schemamodel.View
		want   protocol.SQLView_CheckOption
	}{
		{"a LOCAL check option to an engine that lists levels", checkOptionTarget(),
			schemamodel.View{Name: "recent", Body: "SELECT 1", WithCheck: true, LocalCheck: true}, protocol.SQLView_LOCAL},
		{"a check option to an engine that lists levels", checkOptionTarget(), schemamodel.View{Name: "recent", Body: "SELECT 1", WithCheck: true}, protocol.SQLView_CASCADED},
		{"LOCAL without a check option to an engine that lists levels", checkOptionTarget(),
			schemamodel.View{Name: "recent", Body: "SELECT 1", LocalCheck: true}, protocol.SQLView_CHECK_OPTION_NONE},
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

// A declared LOCAL view is refused by name for an engine that does not list
// levels: it reads with_check as the stricter CASCADED check.
func TestViewCheckOptionDeclaration_FailurePath(t *testing.T) {
	c := qt.New(t)

	declared, err := commonObjectTarget(everyCommonKind...).declaredCommonObjects(&schemamodel.Database{Views: []schemamodel.View{
		{Name: "recent", Body: "SELECT 1", WithCheck: true, LocalCheck: true}}})

	c.Assert(err, qt.ErrorMatches, `.*WITH LOCAL CHECK OPTION of view "recent"`)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(declared, qt.IsNil)
}

// A SQL schema reading keeps the level each view states. A view from an
// engine that does not list levels states none, and its with_check is the
// CASCADED level.
func TestViewCheckOptionReading(t *testing.T) {
	for _, test := range []struct {
		name          string
		withCheck     bool
		option        protocol.SQLView_CheckOption
		wantWithCheck bool
		wantLocal     bool
	}{
		{name: "a CASCADED check option", withCheck: true, option: protocol.SQLView_CASCADED, wantWithCheck: true},
		{name: "a LOCAL check option", withCheck: true, option: protocol.SQLView_LOCAL, wantWithCheck: true, wantLocal: true},
		{name: "a check option without a level", withCheck: true, option: protocol.SQLView_CHECK_OPTION_NONE, wantWithCheck: true},
		{name: "no check option", option: protocol.SQLView_CHECK_OPTION_NONE},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			reply := fullSQLSchemaReply()
			reply.Views[0].WithCheck, reply.Views[0].CheckOption = test.withCheck, test.option

			database, err := sqlSchemaCommon(reply, nil)

			c.Assert(err, qt.IsNil)
			c.Assert(database.Views[0].WithCheck, qt.Equals, test.wantWithCheck)
			c.Assert(database.Views[0].LocalCheck, qt.Equals, test.wantLocal)
		})
	}
}

// The comparison holds a declared view as the provider prepared it, with the
// level the prepared view states rather than the one declared.
func TestViewCheckOptionSpelling(t *testing.T) {
	for _, test := range []struct {
		name          string
		declaredLocal bool
		prepared      protocol.SQLView_CheckOption
		wantLocal     bool
	}{
		{name: "prepared LOCAL", prepared: protocol.SQLView_LOCAL, wantLocal: true},
		{name: "prepared CASCADED", declaredLocal: true, prepared: protocol.SQLView_CASCADED},
		{name: "prepared without a level", declaredLocal: true, prepared: protocol.SQLView_CHECK_OPTION_NONE},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			source := &schemamodel.Database{Views: []schemamodel.View{{Name: "recent", Body: "SELECT 1", WithCheck: true, LocalCheck: test.declaredLocal}}}
			prepared := &protocol.DeclaredCommonObjects{Views: []*protocol.SQLView{{Name: "recent", Body: "SELECT 1", WithCheck: true, CheckOption: test.prepared}}}

			spelled := spelledCommonObjects(source, prepared)

			c.Assert(spelled.Views[0].WithCheck, qt.IsTrue)
			c.Assert(spelled.Views[0].LocalCheck, qt.Equals, test.wantLocal)
			c.Assert(source.Views[0].LocalCheck, qt.Equals, test.declaredLocal)
		})
	}
}

// The host lists the feature, so a provider that offers it keeps it in the
// negotiated description and receives each view's level.
func TestHostListsViewCheckOptions(t *testing.T) {
	c := qt.New(t)
	c.Assert(hostFeatures, qt.Contains, provider.ViewCheckOptionsFeature)
}
