// Package rolepassword keeps a declared role password out of everything Ptah
// prints or writes. The statement that sets a password names an environment
// variable where the password would stand, so a plan, a saved plan, a
// migration file and an error show the variable; only the execution that runs
// the statement receives the value (ADR 0071).
//
// The value comes from one of two places. A plan computed from a declared
// schema in the same run carries the declared passwords in its context, see
// [With]; a saved plan, a migration file and a replay carry none, and the
// variable the statement names is read from the environment. [Lookup] answers
// in that order. The renderers and the executors that write and read the
// reference live with their engines; this package names the variable and
// carries the values.
package rolepassword

import (
	"context"
	"fmt"
	"maps"
	"os"
	"strings"

	"ptah.run/core/schemamodel"
)

// Prefix starts the name of every variable a declared password is read from.
// It is a `PTAH_SECRET_` name, the prefix an operator exports a YDB secret's
// value under, so the one rule "Ptah reads a value into a statement only from
// a PTAH_SECRET_ variable" covers both.
const Prefix = "PTAH_SECRET_PASSWORD_"

// Variable is the environment variable the password declared for role is
// read from: [Prefix] followed by the role's name in upper case, with every
// character a variable name cannot hold written as an underscore. A YDB user
// name holds only lower-case letters and digits, so no two YDB users share a
// variable; [Values] refuses two roles of another engine that do.
func Variable(role string) string {
	var b strings.Builder
	b.WriteString(Prefix)
	for _, r := range strings.ToUpper(role) {
		if r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
			b.WriteRune(r)
			continue
		}
		b.WriteByte('_')
	}
	return b.String()
}

// Reference is how a statement names the variable of role where the password
// would stand: `$` followed by [Variable]. Neither YDB nor PostgreSQL takes it
// as a password, so a statement run without Ptah fails instead of setting
// another password.
func Reference(role string) string {
	return "$" + Variable(role)
}

// ReferencePrefix starts every reference an executor writes a value in place
// of: `$PTAH_SECRET_`. A declared password's reference is one ([Reference]);
// a statement written by hand may name any variable under the prefix, as a
// YDB secret's statement may.
const ReferencePrefix = "$PTAH_SECRET_"

// ReferenceVariable returns the variable token names when it is a reference:
// [ReferencePrefix] in any case, then a name of ASCII letters, digits and
// underscores. ok is false for any other token. A spelling in another case is
// reported as a reference too, so an executor refuses it rather than leaving
// it for the server to read.
func ReferenceVariable(token string) (variable string, ok bool) {
	if len(token) <= len(ReferencePrefix) || !strings.EqualFold(token[:len(ReferencePrefix)], ReferencePrefix) {
		return "", false
	}
	for _, r := range token[1:] {
		if r != '_' && (r < 'A' || r > 'Z') && (r < 'a' || r > 'z') && (r < '0' || r > '9') {
			return "", false
		}
	}
	return token[1:], true
}

// Named reports the variable a declared password names instead of a value:
// a password written as a reference, `$` followed by a variable under the
// PTAH_SECRET_ prefix in exactly that case, such as the one a migration file
// holds, or one a schema writes to keep the value out of itself. ok is false
// for every other password, which is the value.
func Named(password string) (variable string, ok bool) {
	variable, ok = ReferenceVariable(password)
	if !ok || !strings.HasPrefix(variable, ReferencePrefix[1:]) {
		return "", false
	}
	return variable, true
}

// Statement is the reference a statement names role's declared password by:
// the variable the password names itself ([Named]), and otherwise
// [Reference].
func Statement(role, password string) string {
	if variable, named := Named(password); named {
		return "$" + variable
	}
	return Reference(role)
}

// Values maps the variable of every role in roles that declares a password,
// a hash included, to that password. A password that names a variable
// ([Named]) has no value here: the execution reads that variable. It refuses
// two roles whose names share a variable, since one variable cannot hold two
// passwords; the error names the roles and the variable and never a password.
func Values(roles []schemamodel.Role) (map[string]string, error) {
	values := make(map[string]string)
	owners := make(map[string]string)
	for _, role := range roles {
		if _, named := Named(role.Password); role.Password == "" || named {
			continue
		}
		variable := Variable(role.Name)
		if owner, taken := owners[variable]; taken && owner != role.Name {
			return nil, fmt.Errorf("roles %q and %q read their passwords from one environment variable, %s; "+
				"rename one of them", owner, role.Name, variable)
		}
		owners[variable] = role.Name
		values[variable] = role.Password
	}
	return values, nil
}

// Declared is [Values] of the roles desired declares, and none for no
// desired schema.
func Declared(desired *schemamodel.Database) (map[string]string, error) {
	if desired == nil {
		return nil, nil
	}
	return Values(desired.Roles)
}

type contextKey struct{}

// With returns a context carrying values, the declared passwords by their
// variables, for the executions that run under it. It adds to what ctx
// already carries, and a value of values wins. An empty values returns ctx.
func With(ctx context.Context, values map[string]string) context.Context {
	if len(values) == 0 {
		return ctx
	}
	carried, _ := ctx.Value(contextKey{}).(map[string]string)
	merged := maps.Clone(carried)
	if merged == nil {
		merged = make(map[string]string, len(values))
	}
	maps.Copy(merged, values)
	return context.WithValue(ctx, contextKey{}, merged)
}

// Lookup is how an execution reads the value of an input it names: a value
// ctx carries, see [With], and otherwise the environment variable of that
// name. A variable that is set and empty is the empty value; one that is not
// set is not found.
func Lookup(ctx context.Context) func(string) (string, bool) {
	carried, _ := ctx.Value(contextKey{}).(map[string]string)
	return func(name string) (string, bool) {
		if value, declared := carried[name]; declared {
			return value, true
		}
		return os.LookupEnv(name)
	}
}
