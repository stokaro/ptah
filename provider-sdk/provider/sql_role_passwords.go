package provider

import (
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	protocol "ptah.run/provider-sdk/protocol/experimental"
)

// SQLRolePasswordsFeature enables SQLRole.password in a ReadSQLSchema reply:
// the password a SQL schema document gives a role, as the document writes it,
// a literal or a reference to an execution input. The host holds it as it
// holds the password a Go, YAML or HCL source declares, so a plan binds it at
// execution and never prints it. The password travels only in that reply: a
// role a host sends, in declared common objects or in a role change, carries
// none, and a role change carries its password in RoleCreation or
// RoleAlteration. A two-way feature: the provider lists it, and so does each
// engine whose SQL reading returns passwords, beside ReadSQLSchemaFeature and
// the common-object wire with roles. Contract 25.33.
const SQLRolePasswordsFeature = "source.sql-role-passwords" // #nosec G101 -- a feature name, not a credential

// validateSQLRolePasswordsFeature refuses an engine that lists the feature
// without the provider's, without reading SQL documents itself, or without
// carrying roles on the common-object wire.
func validateSQLRolePasswordsFeature(description *protocol.Description, engine *protocol.Engine) error {
	if !slices.Contains(engine.GetFeatures(), SQLRolePasswordsFeature) {
		return nil
	}
	if !slices.Contains(description.GetFeatures(), SQLRolePasswordsFeature) {
		return sqlSchemaError("role passwords require the provider's " + SQLRolePasswordsFeature)
	}
	if !slices.Contains(engine.GetFeatures(), ReadSQLSchemaFeature) || !engineCommonKinds(engine)[protocol.CommonObjectSupport_ROLE] {
		return sqlSchemaError("role passwords require the engine's SQL reading and roles on the common-object wire")
	}
	return nil
}

// validateSQLRolePasswords checks the passwords of the roles a SQL reading
// returns: a password only under the negotiated feature, where a role carries
// one exactly when it declares one, and a password free of control
// characters. Its errors name the role, never the password.
func validateSQLRolePasswords(description *protocol.Description, engine *protocol.Engine, features map[string]bool, roles []*protocol.SQLRole) error {
	negotiated := EngineOffers(description, engine.GetId(), SQLRolePasswordsFeature) && features[SQLRolePasswordsFeature]
	for _, role := range roles {
		password := role.GetPassword()
		switch {
		case password != "" && !negotiated:
			return sqlSchemaError("role " + role.GetName() + " carries a password, and " + SQLRolePasswordsFeature + " was not negotiated")
		case negotiated && role.GetPasswordDeclared() != (password != ""):
			return sqlSchemaError("role " + role.GetName() + " carries a password exactly when it declares one")
		case !validPasswordText(password):
			return sqlSchemaError("the password of role " + role.GetName() + " holds a control character or is not UTF-8")
		}
	}
	return nil
}

// validPasswordText reports whether a password is valid UTF-8 without control
// characters or line separators. An empty password is valid.
func validPasswordText(password string) bool {
	return utf8.ValidString(password) && !strings.ContainsFunc(password, func(r rune) bool {
		return unicode.IsControl(r) || r == ' ' || r == ' '
	})
}
