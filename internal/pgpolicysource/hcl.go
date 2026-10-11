package pgpolicysource

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/core/hclext"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpolicy"
)

// The HCL blocks the owner reads: a top-level policy, and a table's
// row_security switches.
const (
	hclPolicyBlock      = "policy"
	hclRowSecurityBlock = "row_security"
)

// HCL is the row-security owner's contribution to the HCL frontend. Atlas HCL
// declares PostgreSQL's row-level security only, so a `policy` block is a
// policy on the table its `on` names, and a table's `row_security` block is
// the switches facet of that table. The claim is [Coverage] of the policies
// the document declares.
func HCL() hclext.Extension {
	return hclext.Extension{
		Owner: pgpolicy.Owner,
		Kinds: []schemaext.Kind{pgpolicy.PolicyKind, pgpolicy.TableStateKind},
		Blocks: []hclext.Block{
			{Site: hclext.TopLevel, Type: hclPolicyBlock, Naming: hclext.JoinedName, Attributes: []hclext.Attribute{
				{Name: "on", Kind: hclext.Ref, Refers: "table"},
				{Name: "as", Kind: hclext.Text},
				{Name: "for", Kind: hclext.Text},
				{Name: "to", Kind: hclext.Refs, Refers: "role"},
				{Name: "using", Kind: hclext.Text},
				{Name: "check", Kind: hclext.Text},
				{Name: "comment", Kind: hclext.Text},
			}},
			{Site: hclext.InTable, Type: hclRowSecurityBlock, Attributes: []hclext.Attribute{
				{Name: "enabled", Kind: hclext.Bool},
				{Name: "enforced", Kind: hclext.Bool},
				{Name: "comment", Kind: hclext.Text},
			}},
		},
		Decoder: hclDecoder{},
		Coverage: func(request hclext.CoverageRequest) (schemaext.Coverage, error) {
			return Coverage(request.Declared)
		},
	}
}

// hclDecoder reads a document's policies and switches.
type hclDecoder struct{}

// hclRead is one claim read on its own: a policy or a table's switches.
type hclRead struct {
	policy   *pgpolicy.DesiredPolicy
	switches *pgpolicy.DesiredTableState
}

// DecodeHCL reads every claim on its own first, refusing the first one that
// is wrong, and then collects them, policies before switches, refusing two
// that declare one policy or one table's switches, naming both.
func (hclDecoder) DecodeHCL(_ context.Context, request hclext.Request) (hclext.Result, error) {
	read := make([]hclRead, len(request.Claims))
	for i, claim := range request.Claims {
		var err error
		if read[i], err = readHCLClaim(claim); err != nil {
			return hclext.Result{}, &hclext.ClaimError{Claim: i, Err: err}
		}
	}
	declarations := make([]hclext.Declaration, len(request.Claims))
	var collector Collector
	for i, claim := range request.Claims {
		if read[i].policy == nil {
			continue
		}
		on := claim.Attributes["on"].Reference
		ref := Ref(on.Schema, on.Name, claim.Name)
		if err := collector.AddPolicy(fmt.Sprintf("%s %q at %s", hclPolicyBlock, claim.Name, claim.At), ref, *read[i].policy, nil); err != nil {
			return hclext.Result{}, err
		}
		object, _, err := collector.Objects().Get(ref)
		if err != nil {
			return hclext.Result{}, err
		}
		declarations[i].Objects = []schemaext.Object{object}
	}
	// The pinned Atlas community binary reads a repeated row_security block
	// at exit 0 and drops it. A repeat that asks for the same switches
	// changes nothing, so it is read the same way; one that differs is
	// refused by the collector rather than dropped.
	seen := make(map[hclext.Table]pgpolicy.DesiredTableState)
	for i, claim := range request.Claims {
		state := read[i].switches
		if state == nil {
			continue
		}
		table := hclext.Table{Schema: claim.Table.Schema, Name: claim.Table.Name}
		if previous, found := seen[table]; found && previous == *state {
			continue
		}
		seen[table] = *state
		origin := fmt.Sprintf("%s of table %q at %s", hclRowSecurityBlock, claim.Table.Name, claim.At)
		facets, err := collector.AddSwitches(origin, TableRef(table.Schema, table.Name), schemaext.Facets{}, *state, nil)
		if err != nil {
			return hclext.Result{}, err
		}
		declarations[i].Facets = facets
	}
	return hclext.Result{Declarations: declarations}, nil
}

// readHCLClaim reads one block on its own.
func readHCLClaim(claim hclext.Claim) (hclRead, error) {
	if claim.Block == hclRowSecurityBlock {
		state, err := hclSwitches(claim)
		return hclRead{switches: state}, err
	}
	policy, err := hclPolicy(claim)
	return hclRead{policy: policy}, err
}

// hclPolicy reads a policy block. Its `as` selects how the policy combines
// with the table's others rather than how it is spelled, so a value that is
// neither PERMISSIVE nor RESTRICTIVE is refused rather than read as the
// permissive default: the default is the weaker of the two, and guessing it
// turns a misspelled RESTRICTIVE into a policy that grants what its author
// wrote it to withhold.
func hclPolicy(claim hclext.Claim) (*pgpolicy.DesiredPolicy, error) {
	restrictive := false
	if as, written := claim.Attributes["as"]; written {
		switch strings.ToUpper(strings.TrimSpace(as.Text)) {
		case "PERMISSIVE":
		case "RESTRICTIVE":
			restrictive = true
		default:
			return nil, fmt.Errorf("%s %q as must be PERMISSIVE or RESTRICTIVE", hclPolicyBlock, claim.Name)
		}
	}
	roles := claim.Attributes["to"].References
	selectors := make([]string, 0, len(roles))
	for _, role := range roles {
		selectors = append(selectors, hclRole(role))
	}
	policy, err := Attributes{
		For: claim.Attributes["for"].Text, To: strings.Join(selectors, ","),
		Using: claim.Attributes["using"].Text, WithCheck: claim.Attributes["check"].Text,
		Restrictive: restrictive, Comment: claim.Attributes["comment"].Text,
	}.Policy()
	if err != nil {
		return nil, fmt.Errorf("%s %q: %w", hclPolicyBlock, claim.Name, err)
	}
	return &policy, nil
}

// hclRole spells one role a policy applies to in [ParseRoleList]'s grammar.
// HCL writes a role by its exact name, so a name is quoted; a name spelling a
// role keyword is that keyword, as the writer spells one. A role reference
// that names a schema names the role written with the dot, since a role is
// not in a schema.
func hclRole(role hclext.Reference) string {
	name := role.Name
	if role.Schema != "" {
		name = role.Schema + "." + role.Name
	}
	if SpellsKeyword(name) {
		return name
	}
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// hclSwitches reads a table's row_security block, which turns row-level
// security on: a block that leaves it off is refused, since leaving the
// block out is how a table keeps it off.
func hclSwitches(claim hclext.Claim) (*pgpolicy.DesiredTableState, error) {
	if !claim.Attributes["enabled"].Bool {
		return nil, fmt.Errorf("%s requires enabled = true", hclRowSecurityBlock)
	}
	return &pgpolicy.DesiredTableState{Enabled: true, Forced: claim.Attributes["enforced"].Bool,
		Comment: claim.Attributes["comment"].Text, StructName: claim.Table.Struct}, nil
}
