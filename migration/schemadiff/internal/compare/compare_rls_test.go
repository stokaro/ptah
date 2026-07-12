package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/schemadiff/internal/compare"
)

func TestRLSPolicyDefinitions_NormalizesRedundantExpressionParentheses(t *testing.T) {
	c := qt.New(t)

	gen := goschema.RLSPolicy{
		Name:                "users_visible",
		Table:               "auth.users",
		PolicyFor:           "ALL",
		ToRoles:             "PUBLIC",
		UsingExpression:     "id IS NOT NULL",
		WithCheckExpression: "account_id IS NOT NULL",
	}
	db := types.DBRLSPolicy{
		Name:                "users_visible",
		Table:               "auth.users",
		PolicyFor:           "ALL",
		ToRoles:             "PUBLIC",
		UsingExpression:     "(id IS NOT NULL)",
		WithCheckExpression: "((account_id IS NOT NULL))",
	}

	diff := compare.RLSPolicyDefinitions(gen, db)

	c.Assert(diff.Changes, qt.HasLen, 0)
}

func TestRLSPolicyDefinitions_DetectsExpressionChangesAfterNormalization(t *testing.T) {
	c := qt.New(t)

	gen := goschema.RLSPolicy{
		Name:                "users_visible",
		Table:               "auth.users",
		PolicyFor:           "ALL",
		ToRoles:             "PUBLIC",
		UsingExpression:     "id IS NOT NULL",
		WithCheckExpression: "account_id IS NOT NULL",
	}
	db := types.DBRLSPolicy{
		Name:                "users_visible",
		Table:               "auth.users",
		PolicyFor:           "ALL",
		ToRoles:             "PUBLIC",
		UsingExpression:     "(id > 0)",
		WithCheckExpression: "(account_id > 0)",
	}

	diff := compare.RLSPolicyDefinitions(gen, db)

	c.Assert(diff.Changes, qt.DeepEquals, map[string]string{
		"using_expression":      "(id > 0) -> id IS NOT NULL",
		"with_check_expression": "(account_id > 0) -> account_id IS NOT NULL",
	})
}
