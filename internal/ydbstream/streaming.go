// Package ydbstream validates and renders YDB streaming-query declarations.
// Native schema rendering and migration planning share these rules.
package ydbstream

import (
	"fmt"
	"strconv"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/ydbexternal"
	"ptah.run/internal/yqlquery"
)

// Running resolves the server default of RUN=TRUE.
func Running(spec ast.StreamingQuerySpec) bool { return spec.Run == nil || *spec.Run }

// Pool resolves the server default resource pool.
func Pool(spec ast.StreamingQuerySpec) string {
	if spec.ResourcePool == "" {
		return "default"
	}
	return spec.ResourcePool
}

// Equal compares persistent settings, normalizing defaults and outer whitespace.
func Equal(a, b ast.StreamingQuerySpec) bool {
	return strings.TrimSpace(a.Text) == strings.TrimSpace(b.Text) && Running(a) == Running(b) && Pool(a) == Pool(b)
}

// Validate checks the body remains one streaming-query statement when wrapped.
// YDB validates the data query itself; Ptah rejects schema operations and text
// that escapes the enclosing DO block before any migration is executed.
func Validate(spec ast.StreamingQuerySpec) error {
	if strings.TrimSpace(spec.Text) == "" {
		return fmt.Errorf("streaming query text is required")
	}
	queries, err := yqlquery.Split(spec.Text)
	if err != nil {
		return err
	}
	for _, query := range queries {
		if query.Kind != yqlquery.Data {
			return fmt.Errorf("streaming query text must contain data statements only")
		}
	}
	wrapped, err := yqlquery.Split("CREATE STREAMING QUERY validation AS DO BEGIN\n" + spec.Text + "\nEND DO;")
	if err != nil {
		return err
	}
	if len(wrapped) != 1 || wrapped[0].Kind != yqlquery.Scheme {
		return fmt.Errorf("streaming query text must stay inside its DO BEGIN ... END DO block")
	}
	return nil
}

// Refuse returns a capability error unless this target can manage streaming queries.
func Refuse(dialect string, caps capability.Capabilities, subject string) error {
	if caps.Has(capability.StreamingQueries) {
		return nil
	}
	return &ptaherr.CapabilityError{Dialect: platform.NormalizeDialect(dialect), Feature: string(capability.StreamingQueries), Err: ptaherr.ErrUnsupportedFeature,
		Message: subject + ": requires target capability streaming_queries (YDB needs EnableStreamingQueries and EnableExternalDataSources on the cluster)"}
}

// Create renders one CREATE STREAMING QUERY, including explicit run and pool settings.
func Create(name string, spec ast.StreamingQuerySpec) string {
	return "CREATE STREAMING QUERY " + ydbexternal.Path(name) + " WITH (" + settings(spec) + ") AS DO BEGIN\n" + strings.TrimSpace(spec.Text) + "\nEND DO;"
}

// Alter changes persistent settings in place. A changed body requires an
// explicit permission because YDB discards aggregation state. Topic offsets
// remain in the checkpoint; no DROP/CREATE fallback is used.
func Alter(name string, desired, current ast.StreamingQuerySpec, allowReset bool) (string, error) {
	textChanged := strings.TrimSpace(desired.Text) != strings.TrimSpace(current.Text)
	if textChanged && !allowReset {
		return "", fmt.Errorf("streaming query %q: changing text resets aggregation state; declare allow_state_reset=true to permit it", name)
	}
	options := settings(desired)
	if textChanged {
		options += ", FORCE = TRUE"
	}
	statement := "ALTER STREAMING QUERY " + ydbexternal.Path(name) + " SET (" + options + ")"
	if textChanged {
		statement += " AS DO BEGIN\n" + strings.TrimSpace(desired.Text) + "\nEND DO"
	}
	return statement + ";", nil
}

// Drop renders a removal; YDB deletes the query's checkpoints with it.
func Drop(name string) string { return "DROP STREAMING QUERY " + ydbexternal.Path(name) + ";" }

func settings(spec ast.StreamingQuerySpec) string {
	return "RUN = " + strings.ToUpper(strconv.FormatBool(Running(spec))) + ", RESOURCE_POOL = " + sqlident.Quote("ydb", Pool(spec))
}
