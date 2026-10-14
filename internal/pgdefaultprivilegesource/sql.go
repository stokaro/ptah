package pgdefaultprivilegesource

import (
	"context"
	"fmt"

	"ptah.run/core/coverage"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/feature/pgdefaultprivilege"
)

const sqlAlterDefault = "alter-default-privileges"

// SQL reads default-privilege operations in document order. Syntax readers
// retain every operation. Schema loading folds each into the declared ACL,
// including ACLs in earlier files, and refuses operations it cannot represent.
func SQL() sqlext.Extension {
	return sqlext.Extension{Owner: pgdefaultprivilege.Owner,
		Targets:  []string{platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.Spanner},
		Kinds:    []schemaext.Kind{pgdefaultprivilege.DefaultPrivilegeKind},
		Patterns: []sqlext.Pattern{{Name: sqlAlterDefault, Site: sqlext.Statement, Words: []string{"ALTER", "DEFAULT", "PRIVILEGES"}}},
		Parser:   privilegeSQL{}, Header: []coverage.Kind{LimitKind}, Coverage: func(request sqlext.CoverageRequest) (schemaext.Coverage, error) {
			return sourceCoverage(request.Limits)
		}}
}

type privilegeSQL struct{}

func (privilegeSQL) ParseSQL(ctx context.Context, request sqlext.Request) (sqlext.Result, error) {
	if ctx == nil {
		return sqlext.Result{}, fmt.Errorf("%w: default privilege SQL requires a context", schemaext.ErrInvalidValue)
	}
	known, err := schemaext.NewObjects(request.Declared...)
	if err != nil {
		return sqlext.Result{}, err
	}
	result := sqlext.Result{Values: make([]sqlext.Value, 0, len(request.Claims))}
	for _, claim := range request.Claims {
		if err := ctx.Err(); err != nil {
			return sqlext.Result{}, err
		}
		if claim.Pattern != sqlAlterDefault {
			return sqlext.Result{}, fmt.Errorf("%w: default privilege SQL pattern %q", schemaext.ErrInvalidValue, claim.Pattern)
		}
		operation, err := newPrivilegeTokens(request.Target, claim).parse()
		if err != nil {
			return sqlext.Result{}, err
		}
		value, updated, err := sqlDeclaration(known, operation)
		if err != nil {
			return sqlext.Result{}, err
		}
		known = updated
		result.Values = append(result.Values, value)
	}
	if err := ctx.Err(); err != nil {
		return sqlext.Result{}, err
	}
	return result, nil
}

func sqlDeclaration(known schemaext.Objects, operation *pgdefaultprivilege.Operation) (sqlext.Value, schemaext.Objects, error) {
	ref, err := pgdefaultprivilege.Ref(operation.Identity, identifier.ForDialect("postgres"))
	if err != nil {
		return sqlext.Value{}, schemaext.Objects{}, err
	}
	object, found, err := known.Get(ref)
	if err != nil {
		return sqlext.Value{}, schemaext.Objects{}, err
	}
	before := &pgdefaultprivilege.Desired{}
	if found {
		var ok bool
		before, ok = object.Value.(*pgdefaultprivilege.Desired)
		if !ok {
			return sqlext.Value{}, schemaext.Objects{}, fmt.Errorf("%w: SQL default privileges require desired values", schemaext.ErrInvalidValue)
		}
	}
	if err := pgdefaultprivilege.ValidateDesired(before); err != nil {
		return sqlext.Value{}, schemaext.Objects{}, err
	}
	value := sqlext.Value{Statement: operation}
	desired, err := applySQLOperation(before, operation)
	if err != nil {
		value.UnmodeledReason = err.Error()
		return value, known, nil
	}
	object, err = pgdefaultprivilege.DesiredObject(ref, *desired)
	if err != nil {
		return sqlext.Value{}, schemaext.Objects{}, err
	}
	if found {
		value.Replaces = []schemaext.Object{object}
		known, err = known.Replace(object)
	} else {
		value.Objects = []schemaext.Object{object}
		known, err = known.With(object)
	}
	return value, known, err
}
