package builtin_test

import (
	"errors"
	"maps"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
	"ptah.run/internal/builtintest"
)

// extensionOwners is where each payload renders in the bundled runtime, as
// target/role. Every target and role not named refuses the payload through the
// common boundary.
var extensionOwners = map[schemaext.Kind][]string{
	"ptah.run/clickhouse/add-skipping-index":              {"clickhouse/alter-table"},
	"ptah.run/clickhouse/alter-ttl":                       {"clickhouse/alter-table"},
	"ptah.run/clickhouse/drop-skipping-index":             {"clickhouse/alter-table"},
	"ptah.run/clickhouse/modify-refresh":                  {"clickhouse/alter-table"},
	"ptah.run/clickhouse/row-policy-operation":            {"clickhouse/statement"},
	"ptah.run/cockroachdb/alter-row-ttl":                  {"cockroachdb/alter-table"},
	"ptah.run/mssql/security-policy-operation":            {"sqlserver/statement"},
	"ptah.run/pgpolicy/policy-comment-operation":          postgresFamilyStatements,
	"ptah.run/pgpolicy/policy-operation":                  postgresFamilyStatements,
	"ptah.run/pgpolicy/table-state-operation":             postgresFamilyStatements,
	"ptah.run/spanner/alter-row-deletion-policy":          {"spanner/alter-table"},
	"ptah.run/timescaledb/continuous-aggregate-operation": postgresFamilyStatements,
	"ptah.run/timescaledb/create-hypertable":              postgresFamilyStatements,
	"ptah.run/ydb/add-changefeed":                         {"ydb/alter-table"},
	"ptah.run/ydb/alter-changefeed-topic":                 {"ydb/alter-table"},
	"ptah.run/ydb/alter-column-families":                  {"ydb/alter-table"},
	"ptah.run/ydb/alter-table-partitioning":               {"ydb/alter-table"},
	"ptah.run/ydb/alter-ttl":                              {"ydb/alter-table"},
	"ptah.run/ydb/async-replication-operation":            {"ydb/statement"},
	"ptah.run/ydb/coordination-node-operation":            {"ydb/statement"},
	"ptah.run/ydb/default-pool-settings-operation":        {"ydb/statement"},
	"ptah.run/ydb/drop-changefeed":                        {"ydb/alter-table"},
	"ptah.run/ydb/external-data-source-operation":         {"ydb/statement"},
	"ptah.run/ydb/external-table-operation":               {"ydb/statement"},
	"ptah.run/ydb/resource-pool-classifier-operation":     {"ydb/statement"},
	"ptah.run/ydb/resource-pool-operation":                {"ydb/statement"},
	"ptah.run/ydb/secret-operation":                       {"ydb/statement"},
	"ptah.run/ydb/streaming-query-operation":              {"ydb/statement"},
	"ptah.run/ydb/topic-consumer-operation":               {"ydb/statement"},
	"ptah.run/ydb/topic-operation":                        {"ydb/statement"},
	"ptah.run/ydb/transfer-operation":                     {"ydb/statement"},
}

// postgresFamilyStatements are the targets that compose the TimescaleDB and
// row-security owners, each in the statement role.
var postgresFamilyStatements = []string{"cockroachdb/statement", "postgres/statement", "spanner/statement", "yugabytedb/statement"}

// TestExtensionPayloads_RouteThroughTheBundledRuntime is the feature-routing
// layer of the guard: every payload the owner packages declare, which the
// fixtures equal (TestExtensionPayloads_CoverSourceTypes), is registered with
// its owner's codec and its owner's validating renderer in the bundled
// runtime, and every target and role that does not own it refuses it through
// the common boundary, with the refusal renderer.UnsupportedExtension builds
// and no SQL.
//
// The targets are the runtime's own, so a target added later is asked too. The
// capabilities are every one the target's preset can hold at once, so an
// owner's capability check cannot read as a refusal at the boundary.
func TestExtensionPayloads_RouteThroughTheBundledRuntime(t *testing.T) {
	runtime := builtintest.Runtime()
	kinds := make([]schemaext.Kind, 0, len(allExtensionFixtures()))
	for _, fixture := range allExtensionFixtures() {
		kind := fixture.payload.Kind()
		kinds = append(kinds, kind)
		t.Run(string(kind), func(t *testing.T) {
			c := qt.New(t)
			data, err := runtime.Codecs().Marshal(c.Context(), schemaext.Operation, []schemaext.Payload{fixture.payload})
			c.Assert(err, qt.IsNil)
			decoded, err := runtime.Codecs().Unmarshal(c.Context(), data)
			c.Assert(err, qt.IsNil)
			c.Assert(decoded, qt.DeepEquals, []schemaext.Payload{fixture.payload})

			answers := routingAnswers(c, runtime, fixture.payload)

			c.Assert(answers.unexpected, qt.HasLen, 0)
			c.Assert(answers.rendered, qt.DeepEquals, extensionOwners[kind])
			c.Assert(len(answers.rendered)+len(answers.refused), qt.Equals, 2*len(runtime.Targets()))
		})
	}
	c := qt.New(t)
	slices.Sort(kinds)
	c.Assert(slices.Sorted(maps.Keys(extensionOwners)), qt.DeepEquals, kinds)
}

// routing is how one payload answered every target and role of a runtime.
type routing struct {
	// rendered are the target/role pairs that returned SQL, sorted.
	rendered []string
	// refused are the pairs that returned exactly the common refusal and no
	// SQL, sorted.
	refused []string
	// unexpected are the remaining pairs, with what each returned.
	unexpected map[string]string
}

func routingAnswers(c *qt.C, runtime *engine.Runtime, payload ast.ExtensionPayload) routing {
	c.Helper()
	answers := routing{unexpected: make(map[string]string)}
	for _, target := range runtime.Targets() {
		for _, role := range []ast.ExtensionRole{ast.StatementExtension, ast.AlterExtension} {
			key := target + "/" + string(role)
			result, err := runtime.Render(c.Context(), renderer.Request{
				Target: target, Capabilities: everyCapability(target), Nodes: []ast.Node{extensionNode(role, payload)},
			})
			switch {
			case err == nil && result.SQL() != "":
				answers.rendered = append(answers.rendered, key)
			case commonRefusal(err, target, payload.Kind(), role) && len(result.Fragments) == 0:
				answers.refused = append(answers.refused, key)
			default:
				answers.unexpected[key] = "SQL " + result.SQL() + ", error " + errors.Join(err).Error()
			}
		}
	}
	return answers
}

// commonRefusal reports whether err carries the refusal the common boundary
// returns for an unregistered target, kind and role.
func commonRefusal(err error, target string, kind schemaext.Kind, role ast.ExtensionRole) bool {
	want, ok := renderer.UnsupportedExtension(target, kind, role).(*ptaherr.CapabilityError)
	got, found := errors.AsType[*ptaherr.CapabilityError](err)
	return ok && found && *got == *want
}

// extensionNode places payload where role says it goes: a standalone statement,
// or an operation of a real ALTER TABLE parent.
func extensionNode(role ast.ExtensionRole, payload ast.ExtensionPayload) ast.Node {
	if role == ast.AlterExtension {
		return &ast.AlterTableNode{Name: "items", Operations: []ast.AlterOperation{&ast.ExtensionAlterOperation{Payload: payload}}}
	}
	return &ast.ExtensionStatement{Payload: payload}
}

// everyCapability is the target's preset with every capability it can hold at
// once turned on, taken in the order capability.All lists them.
func everyCapability(target string) capability.Capabilities {
	caps := capability.ForDialect(target)
	for _, key := range capability.All() {
		if widened := caps.With(key, true); widened.Validate() == nil {
			caps = widened
		}
	}
	return caps
}
