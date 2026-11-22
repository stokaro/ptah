package atlasschema

import (
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"

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
	// TableRebuildDialects names the dialects whose plans create a table again
	// where the engine has no ALTER TABLE for the change: the
	// `diff "<dialect>" { allow_recreate = true }` blocks of an Atlas project
	// file, which the compatibility surface reads. Use [DiffPolicy.AllowsTableRebuild]
	// to ask about one plan.
	TableRebuildDialects []string
	// TableRebuildRequest is how the caller asks for AllowTableRebuild, and the
	// refusal of a change only a rebuild can make names it. Empty names the
	// native flag. See [ptah.run/migration/planext.Options.TableRebuildRequest].
	TableRebuildRequest string

	// Dialects holds, by dialect, the policy a plan for a dialect that a
	// labeled `diff "<dialect>"` block of an Atlas project file names follows
	// in place of SkipChangeKinds and the concurrent index fields above.
	// [DiffPolicy.ForDialect] picks the one for a plan.
	Dialects map[string]DialectDiffPolicy

	// RotateSecrets names the secrets, as the operator wrote them, the plan
	// gives the value their declared variable holds now. A comparison cannot
	// find a changed value, which the server never returns, so this is the
	// only way one is planned. The native commands set it from
	// --rotate-secret.
	RotateSecrets []string
}

// DialectDiffPolicy is the policy a labeled diff block gives a plan for its
// dialect, merged over the unlabeled block's.
type DialectDiffPolicy struct {
	SkipChangeKinds               []planext.ChangeKind
	ConcurrentIndexCreate         bool
	ConcurrentIndexDrop           bool
	ConcurrentIndexCreateDisabled bool
	// Blocks names the policy blocks the labeled block holds, for the refusal
	// of a plan for another dialect.
	Blocks []string
}

// ErrDiffPolicyForAnotherDialect is the refusal of a plan for a dialect other
// than a policy block's label names.
var ErrDiffPolicyForAnotherDialect = errors.New("a labeled diff block holds a policy for another dialect")

// ForDialect returns the policy a plan for the dialect follows: the one its
// labeled diff block gives, where there is one. The result holds no labeled
// policy, so asking again changes nothing.
//
// It refuses, with [ErrDiffPolicyForAnotherDialect], a policy with a labeled
// block for another dialect. The community binary ignores the label and
// applies the block to a plan for any dialect, measured on v1.4.0, so a skip
// that names another dialect is one the user may expect here; refusing keeps
// the plan from dropping what the block asks to leave out.
func (p DiffPolicy) ForDialect(dialect string) (DiffPolicy, error) {
	for _, other := range slices.Sorted(maps.Keys(p.Dialects)) {
		if other == dialect {
			continue
		}
		return DiffPolicy{}, fmt.Errorf("%w: the diff %q block of the project file holds %s, which Ptah applies only to a plan for %s, "+
			"and this plan is for %q. The community binary ignores the label and applies the block to every dialect. "+
			"Move the block into an unlabeled diff block to apply it to every dialect, or into the env of the %s database",
			ErrDiffPolicyForAnotherDialect, other, policyBlockList(p.Dialects[other].Blocks), other, dialect, other)
	}
	if labeled, found := p.Dialects[dialect]; found {
		p.SkipChangeKinds = labeled.SkipChangeKinds
		p.ConcurrentIndexCreate = labeled.ConcurrentIndexCreate
		p.ConcurrentIndexDrop = labeled.ConcurrentIndexDrop
		p.ConcurrentIndexCreateDisabled = labeled.ConcurrentIndexCreateDisabled
	}
	p.Dialects = nil
	return p, nil
}

// policyBlockList names the blocks for a refusal: "a skip block", "a
// concurrent_index block and a skip block".
func policyBlockList(blocks []string) string {
	named := make([]string, len(blocks))
	for i, block := range blocks {
		named[i] = "a " + block + " block"
	}
	return strings.Join(named, " and ")
}

// AllowsTableRebuild reports whether a plan for the dialect creates a table
// again where the engine has no ALTER TABLE for the change: the policy asks for
// it on every dialect, or its project file asks for it on this one.
func (p DiffPolicy) AllowsTableRebuild(dialect string) bool {
	return p.AllowTableRebuild || slices.Contains(p.TableRebuildDialects, dialect)
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
