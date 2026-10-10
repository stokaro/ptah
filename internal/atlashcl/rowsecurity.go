package atlashcl

import (
	"fmt"
	"slices"

	"github.com/hashicorp/hcl/v2/hclsyntax"

	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgpolicy"
	"ptah.run/internal/pgpolicysource"
)

// hclPolicy is one policy block, read and waiting for the table it is on.
type hclPolicy struct {
	origin string
	name   string
	table  string
	policy pgpolicy.DesiredPolicy
}

// hclSwitches is one table's row_security block.
type hclSwitches struct {
	origin string
	schema string
	table  string
	state  pgpolicy.DesiredTableState
}

// blockOrigin names a block in a refusal that has to name two: what it
// declares and where.
func (p *parser) blockOrigin(block *hclsyntax.Block, what string) string {
	return fmt.Sprintf("%s at %s:%d", what, block.TypeRange.Filename, block.TypeRange.Start.Line)
}

// attachRowSecurity hands the document's row-level security to the PostgreSQL
// row-security owner once every table block is read: a policy becomes an
// object on the table its `on` names, and a row_security block the switches
// facet of its table. Atlas HCL declares PostgreSQL's row-level security
// only, so no declaration stays in the shared model. A policy whose table this
// document does not declare keeps the reference as written, since another
// document of the schema may declare it. Two blocks that declare one policy
// are refused, naming both (stokaro/ptah#2440).
func (p *parser) attachRowSecurity() error {
	var collector pgpolicysource.Collector
	for _, declared := range p.policies {
		index, err := pgpolicysource.DeclaredTable(p.db.Tables, "", declared.table)
		if err != nil {
			return fmt.Errorf("%s: %w", declared.origin, err)
		}
		var schema, table string
		if index >= 0 {
			schema, table = p.db.Tables[index].Schema, p.db.Tables[index].Name
		} else if schema, table, err = pgpolicysource.TableParts(declared.table); err != nil {
			return fmt.Errorf("%s: %w", declared.origin, err)
		}
		if err := collector.AddPolicy(declared.origin, pgpolicysource.Ref(schema, table, declared.name), declared.policy, nil); err != nil {
			return err
		}
	}
	read := make(map[int]pgpolicy.DesiredTableState)
	for _, declared := range p.switches {
		index := slices.IndexFunc(p.db.Tables, func(table schemamodel.Table) bool {
			return table.Schema == declared.schema && table.Name == declared.table
		})
		if index < 0 {
			return fmt.Errorf("%s: the table block is not in the document", declared.origin)
		}
		// The pinned binary reads a repeated row_security block at exit 0 and
		// drops it. A repeat that asks for the same switches changes nothing,
		// so it is read the same way; one that differs is refused below
		// rather than dropped.
		if previous, found := read[index]; found && previous == declared.state {
			continue
		}
		read[index] = declared.state
		table := &p.db.Tables[index]
		facets, err := collector.AddSwitches(declared.origin, pgpolicysource.TableRef(table.Schema, table.Name), table.Facets, declared.state, nil)
		if err != nil {
			return err
		}
		table.Facets = facets
	}
	objects, err := p.db.FeatureObjects.Merge(collector.Objects())
	if err != nil {
		return err
	}
	p.db.FeatureObjects = objects
	return nil
}
