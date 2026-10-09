// Package ydbstreaming validates and renders YDB streaming-query declarations.
// Native schema rendering and migration planning share these rules.
package ydbstreaming

import (
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/ydbexternal"
	"ptah.run/internal/yqlquery"
)

// Running resolves the server default of RUN=TRUE.
func Running(spec Spec) bool { return spec.Run == nil || *spec.Run }

// Pool resolves the server default resource pool.
func Pool(spec Spec) string {
	if spec.ResourcePool == "" {
		return "default"
	}
	return spec.ResourcePool
}

// Equal compares persistent settings, normalizing defaults, whitespace, and comments.
func Equal(a, b Spec) bool {
	return SameBody(a.Text, b.Text) && Running(a) == Running(b) && Pool(a) == Pool(b)
}

// Validate checks the body remains one streaming-query statement when wrapped.
// YDB validates the data query itself; Ptah rejects schema operations and text
// that escapes the enclosing DO block before any migration is executed.
func Validate(spec Spec) error {
	if !utf8.ValidString(spec.Text) || !utf8.ValidString(spec.ResourcePool) || strings.ContainsRune(spec.ResourcePool, 0) {
		return fmt.Errorf("streaming query text and resource pool must be valid UTF-8 without a NUL in the pool name")
	}
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

// CreateOptions carries conditional creation and explicit replacement intent.
type CreateOptions struct {
	// OrReplace permits replacing an existing query, resetting aggregation state
	// while retaining topic offsets.
	OrReplace bool
	// IfNotExists preserves an existing query, including with OrReplace.
	IfNotExists bool
}

// ReplacesExisting reports whether creation can reset an existing query's
// aggregation state. Measured on YDB 26.2, IF NOT EXISTS takes precedence over
// OR REPLACE. Parsing, rendering assessments, and SQL lint share this decision.
func (o CreateOptions) ReplacesExisting() bool { return o.OrReplace && !o.IfNotExists }

// Create renders one CREATE STREAMING QUERY, including explicit run and pool settings.
func Create(name string, spec Spec, options CreateOptions) string {
	prefix := "CREATE "
	if options.OrReplace {
		prefix += "OR REPLACE "
	}
	prefix += "STREAMING QUERY "
	if options.IfNotExists {
		prefix += "IF NOT EXISTS "
	}
	return prefix + ydbexternal.Path(name) + " WITH (" + settings(spec) + ") AS DO BEGIN\n" + strings.TrimSpace(spec.Text) + "\nEND DO;"
}

// AlterOptions carries the explicit permission for a destructive body change.
type AlterOptions struct {
	// AllowStateReset permits resetting aggregation state while retaining topic offsets.
	AllowStateReset bool
}

// Alter changes persistent settings in place. A changed body requires an
// explicit permission because YDB discards aggregation state. Topic offsets
// remain in the checkpoint; no DROP/CREATE fallback is used.
func Alter(name string, desired, current Spec, options AlterOptions) (string, error) {
	if err := ValidateAlter(name, desired, current, options); err != nil {
		return "", err
	}
	textChanged := !SameBody(desired.Text, current.Text)
	settingsSQL := settings(desired)
	if textChanged {
		settingsSQL += ", FORCE = TRUE"
	}
	statement := "ALTER STREAMING QUERY " + ydbexternal.Path(name) + " SET (" + settingsSQL + ")"
	if textChanged {
		statement += " AS DO BEGIN\n" + strings.TrimSpace(desired.Text) + "\nEND DO"
	}
	return statement + ";", nil
}

// Drop renders a removal; YDB deletes the query's checkpoints with it.
func Drop(name string) string { return "DROP STREAMING QUERY " + ydbexternal.Path(name) + ";" }

func settings(spec Spec) string {
	return "RUN = " + strings.ToUpper(strconv.FormatBool(Running(spec))) + ", RESOURCE_POOL = " + sqlident.Quote("ydb", Pool(spec))
}

// ValidateAlter checks reset permission without rendering. Both typed operation
// validation and SQL generation use this predicate so they cannot disagree.
func ValidateAlter(name string, desired, current Spec, options AlterOptions) error {
	if !SameBody(desired.Text, current.Text) && !options.AllowStateReset {
		return fmt.Errorf("streaming query %q: changing text resets aggregation state; declare allow_state_reset=true to permit it", name)
	}
	return nil
}
