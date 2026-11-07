package atlasschema

import (
	"fmt"
	"io"
	"slices"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/difftypes"
	"ptah.run/core/planext"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/concurrentindex"
	"ptah.run/internal/sqlitevirtual"
	"ptah.run/migration/diffpolicy"
)

// DiffPolicy holds Atlas-compatible schema diff policy that maps to Ptah's
// local planning capabilities.
type DiffPolicy struct {
	// SkipChangeKinds lists the destructive change kinds a plan leaves out:
	// `diff.skip` in ptah.yaml, and `diff { skip { ... } }` in an Atlas
	// project file. They are removed by [diffpolicy.ApplyForDialect], the
	// filter `ptah migrations generate` applies, so both commands omit the same
	// changes, and the plan carries one comment for each change it left out.
	SkipChangeKinds []planext.ChangeKind

	ConcurrentIndexCreate bool
	ConcurrentIndexDrop   bool
	// OnlineAlter asks the server to apply the plan without blocking the
	// writes already running against its tables, in whichever grammar the
	// target has. A target without one is planned as if this were off.
	OnlineAlter bool

	// ConcurrentIndexCreateDisabled is the operator saying no, which is a
	// different answer from not saying yes.
	//
	// ConcurrentIndexCreate builds EVERY addition concurrently. Its zero value
	// does not mean the opposite: it means nothing was requested, and a desired
	// description that asked for `CREATE INDEX CONCURRENTLY` is still honored
	// (stokaro/ptah#2019). Only this field turns that off, and it is set from an
	// explicit `false` in the project config rather than from an absent one --
	// which is why the config's presence bit is read and not just its value.
	ConcurrentIndexCreateDisabled bool

	// AllowTableRebuild plans a change the target cannot make in place as an
	// explicit table rebuild. The native commands set it from
	// --allow-table-rebuild, and the compatibility surface, which takes no new
	// flag, from PTAH_ALLOW_TABLE_REBUILD.
	AllowTableRebuild bool
	// TableRebuildRequest is how the caller asks for AllowTableRebuild, and the
	// refusal of a change only a rebuild can make names it. Empty names the
	// native flag. See [ptah.run/migration/planext.Options.TableRebuildRequest].
	TableRebuildRequest string

	// RotateSecrets names the secrets, as the operator wrote them, the plan
	// gives the value their declared variable holds now. A comparison cannot
	// find a changed value, which the server never returns, so this is the
	// only way one is planned. The native commands set it from
	// --rotate-secret.
	RotateSecrets []string
}

// withSecretRotation adds to opts a rotation request for each secret policy
// names, as written: the secret's owner reads the spelling, and decides
// whether each is a rotation, a creation, or undecided, and the comparison
// refuses a spelling no owner reads and a name the declaration does not hold.
func withSecretRotation(opts *config.CompareOptions, policy DiffPolicy) {
	opts.FeatureRequests = append(opts.FeatureRequests, schemaext.WrittenRequests(schemaext.RotateAction, policy.RotateSecrets)...)
}

// declaredConcurrentIndexRefs is the index additions the desired description
// asked to build concurrently, subject to the operator's mode.
//
// The two early returns are the two cases where the declaration changes
// nothing: the operator turned concurrent builds off, and an instruction is not
// overruled by a description; or the operator turned them on for every
// addition, which already includes these.
//
// Capabilities are resolved to the dialect default when the caller pinned no
// server version, because that is what the planner does with the same nil and
// the two must not disagree. Reading the nil through [capability.Capabilities.Has]
// instead answers false for every key, which would leave the declaration
// silently unhonored on exactly the invocation that pins no version -- the
// common one.
func declaredConcurrentIndexRefs(
	policy DiffPolicy,
	diff *difftypes.SchemaDiff,
	desired *schemamodel.Database,
	current *catalog.Database,
	dialect string,
	capabilities capability.Capabilities,
) []difftypes.IndexRef {
	if policy.ConcurrentIndexCreateDisabled || policy.ConcurrentIndexCreate {
		return nil
	}
	if capabilities.Len() == 0 {
		capabilities = capability.ForDialect(dialect)
	}
	return concurrentindex.DeclaredRefs(diff, desired, current, catalog.ServerInfo{
		Dialect:      dialect,
		Capabilities: capabilities,
	})
}

// ApplyDiffPolicy returns diff without the changes of the kinds
// policy.SkipChangeKinds lists, and the changes it left out in the order a plan
// names them. A skipped table drop takes the removals that belong to the kept
// table with it; [diffpolicy.ApplyForDialect] is the filter and says which.
// dialect selects the index-name namespace a standalone index drop is told
// from a replacement in. diff is not changed, and a policy that skips nothing
// returns it as given.
func ApplyDiffPolicy(diff *difftypes.SchemaDiff, policy DiffPolicy, dialect string) (*difftypes.SchemaDiff, []diffpolicy.SkippedChange) {
	return diffpolicy.ApplyForDialect(diff, diffpolicy.NewSkipSet(policy.SkipChangeKinds...), dialect)
}

// CompareOptions returns the default comparison options, told which removals
// the caller deletes from the comparison's answer under policy.
//
// The comparison reads them only in the SQLite virtual-table guard, which runs
// inside it and refuses on the statements it predicts. A drop the policy
// deletes afterwards is a statement nothing runs, so without this the guard
// refuses a plan the policy has already emptied (stokaro/ptah#1028). Every
// kind the guard reads is forwarded; see [config.CompareOptions.SkipTableDrops].
func (p DiffPolicy) CompareOptions() *config.CompareOptions {
	opts := config.DefaultCompareOptions()
	opts.SkipTableDrops = slices.Contains(p.SkipChangeKinds, planext.DropTable)
	opts.SkipColumnDrops = slices.Contains(p.SkipChangeKinds, planext.DropColumn)
	opts.SkipIndexDrops = slices.Contains(p.SkipChangeKinds, planext.DropIndex)
	return opts
}

// virtualTablePolicy is the policy as the SQLite virtual-table guard reads it,
// for a comparison that runs the guard itself rather than through
// [DiffPolicy.CompareOptions].
func (p DiffPolicy) virtualTablePolicy() sqlitevirtual.Policy {
	opts := p.CompareOptions()
	return sqlitevirtual.Policy{
		SkipDropTable:  opts.SkipTableDrops,
		SkipDropColumn: opts.SkipColumnDrops,
		SkipDropIndex:  opts.SkipIndexDrops,
	}
}

// ReportOmitted writes one line for each change the diff policy left out, in
// the words the plan's comment carries. A command whose plan has no statement
// left to carry the comment writes it beside its result instead, so "synced"
// never stands alone over a change the policy withheld.
func ReportOmitted(diagnostics io.Writer, omitted []diffpolicy.SkippedChange) {
	if diagnostics == nil {
		return
	}
	for _, change := range omitted {
		_, _ = fmt.Fprintln(diagnostics, change.Comment())
	}
}
