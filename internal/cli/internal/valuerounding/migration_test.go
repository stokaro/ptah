package valuerounding_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/internal/valuerounding"
	"ptah.run/migration/migrator"
)

func TestMigrationPolicy_NativeInvocation(t *testing.T) {
	for _, allowed := range []bool{false, true} {
		t.Run(map[bool]string{false: "refuse", true: "allow"}[allowed], func(t *testing.T) {
			c := qt.New(t)
			c.Assert(valuerounding.MigrationPolicy(context.Background(), allowed), qt.Equals,
				migrator.ValueRounding{Allowed: allowed, Request: "--allow-value-rounding"})
		})
	}
}

func TestMigrationPolicy_AdapterOwnsTheChoice(t *testing.T) {
	tests := []struct {
		name    string
		adapter bool
		native  bool
	}{
		{"adapter allows", true, false},
		{"adapter refuses", false, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			policy := migrator.ValueRounding{Allowed: test.adapter, Request: "PTAH_ALLOW_VALUE_ROUNDING=1"}
			ctx := valuerounding.WithMigrationPolicy(context.Background(), policy)
			c.Assert(valuerounding.MigrationPolicy(ctx, test.native), qt.Equals, policy)
		})
	}
}
