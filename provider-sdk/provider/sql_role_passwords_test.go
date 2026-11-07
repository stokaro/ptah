package provider_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

// passwordSecret is the password the exchanges below carry. No refusal may
// repeat it.
const passwordSecret = "s3cret-pw"

// passwordExchange is sqlSchemaExchange for an engine that carries roles on
// the common-object wire and returns the passwords its SQL reading finds,
// with a reply holding one user whose document gives it password, under a
// target that lists the feature when listed is true.
func passwordExchange(password string, declared, listed bool) (*protocol.Description, *protocol.ReadSQLSchemaRequest, *protocol.SQLSchema) {
	return passwordExchangeFor(password, declared, map[bool][]string{
		true:  {provider.CommonObjectsFeature, provider.SQLRolePasswordsFeature},
		false: {provider.CommonObjectsFeature},
	}[listed])
}

// passwordExchangeFor is passwordExchange under the target features given.
func passwordExchangeFor(password string, declared bool, targetFeatures []string) (*protocol.Description, *protocol.ReadSQLSchemaRequest, *protocol.SQLSchema) {
	d, request, result := sqlSchemaExchange()
	d.Features = append(d.Features, provider.RenderFeature, provider.ValidateSchemaFeature, provider.CommonObjectsFeature, provider.SQLRolePasswordsFeature)
	engine := d.Engines[0]
	engine.Features = append(engine.Features, provider.ValidateSchemaFeature, provider.CommonObjectsFeature, provider.SQLRolePasswordsFeature)
	engine.CommonObjects = &protocol.CommonObjectSupport{Kinds: allCommonKinds()}
	request.TargetFeatures = targetFeatures
	result.Roles = []*protocol.SQLRole{{Name: "app", Login: true, PasswordDeclared: declared, Password: password}}
	return d, request, result
}

func TestSQLRolePasswords_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name     string
		password string
		declared bool
		listed   bool
	}{
		{"a literal password", passwordSecret, true, true},
		{"a reference to an execution input", "$PTAH_SECRET_APP_PASSWORD", true, true},
		{"a user without a password", "", false, true},
		{"a declared password for a target that does not list the feature", "", true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			description, request, result := passwordExchange(test.password, test.declared, test.listed)

			c.Assert(provider.ValidateDescription(description, nil), qt.IsNil)
			c.Assert(provider.ValidateSQLSchema(description, request, result), qt.IsNil)
		})
	}
}

// A reading carries a password only under the feature, with
// password_declared, and as one line of text; a refusal names the role and
// never the password.
func TestSQLRolePasswords_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name     string
		password string
		declared bool
		listed   bool
		want     string
	}{
		{"a password for a target that does not list the feature", passwordSecret, true, false,
			`.*role app carries a password, and source.sql-role-passwords was not negotiated`},
		{"a password without password_declared", passwordSecret, false, true, `.*role app carries a password exactly when it declares one`},
		{"password_declared without the password", "", true, true, `.*role app carries a password exactly when it declares one`},
		{"a control character", passwordSecret + "\n", true, true, `.*the password of role app holds a control character or is not UTF-8`},
		{"invalid UTF-8", passwordSecret + "\xff", true, true, `.*the password of role app holds a control character or is not UTF-8`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			description, request, result := passwordExchange(test.password, test.declared, test.listed)

			err := provider.ValidateSQLSchema(description, request, result)

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err.Error(), qt.Not(qt.Contains), passwordSecret)
		})
	}
}

// A role a host sends never carries a password: neither in declared common
// objects nor in a role change, which carries its password beside the role.
func TestSQLRolePasswordsHostMessages_FailurePath(t *testing.T) {
	t.Run("declared common objects", func(t *testing.T) {
		c := qt.New(t)
		request := prepareRequest()
		request.Declared.Roles[1].Password = passwordSecret

		err := provider.ValidatePrepareCommonObjectsRequest(commonDescription(), request)

		c.Assert(err, qt.ErrorMatches, `.*role app carries a password, which travels only in a SQL reading`)
		c.Assert(err.Error(), qt.Not(qt.Contains), passwordSecret)
	})
	t.Run("a prepared declaration", func(t *testing.T) {
		c := qt.New(t)
		prepared := preparedCommon()
		prepared.Prepared.Roles[1].Password = passwordSecret

		err := provider.ValidatePreparedCommonObjects(commonDescription(), prepareRequest(), prepared)

		c.Assert(err, qt.ErrorMatches, `.*role app carries a password, which travels only in a SQL reading`)
	})
}

func TestSQLRolePasswordsDescription_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*protocol.Description)
		want   string
	}{
		{"an engine feature without the provider's", func(d *protocol.Description) {
			d.Features = slices.DeleteFunc(d.Features, func(feature string) bool { return feature == provider.SQLRolePasswordsFeature })
		}, `.*role passwords require the provider's source.sql-role-passwords`},
		{"an engine without roles on the common-object wire", func(d *protocol.Description) {
			d.Engines[0].CommonObjects.Kinds = kindsWithout(protocol.CommonObjectSupport_ROLE)
		}, `.*role passwords require the engine's SQL reading and roles on the common-object wire`},
		{"a FRAGMENTS engine", func(d *protocol.Description) {
			d.Features = slices.DeleteFunc(d.Features, func(feature string) bool { return feature == provider.ReadSQLSchemaFeature })
			d.Engines[0].Features = slices.DeleteFunc(d.Engines[0].Features, func(feature string) bool { return feature == provider.ReadSQLSchemaFeature })
			d.Engines[0].SqlSchema = &protocol.SQLSchemaDeclaration{Mode: protocol.SQLSchemaDeclaration_FRAGMENTS}
		}, `.*role passwords require the engine's SQL reading and roles on the common-object wire`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			description, _, _ := passwordExchange(passwordSecret, true, true)
			test.mutate(description)

			c.Assert(provider.ValidateDescription(description, nil), qt.ErrorMatches, test.want)
		})
	}
}

// A host that does not list the feature negotiates it away, so a reading that
// still carries a password is refused as unnegotiated.
func TestSQLRolePasswordsNegotiation_FailurePath(t *testing.T) {
	c := qt.New(t)
	description, request, result := passwordExchange(passwordSecret, true, false)
	negotiated := provider.NegotiatedDescription(description, slices.DeleteFunc(provider.TwoWayFeatures(), func(feature string) bool {
		return feature == provider.SQLRolePasswordsFeature
	}))

	c.Assert(provider.ValidateDescription(negotiated, nil), qt.IsNil)
	c.Assert(provider.ValidateSQLSchema(negotiated, request, result), qt.ErrorMatches,
		`.*role app carries a password, and source.sql-role-passwords was not negotiated`)
}
