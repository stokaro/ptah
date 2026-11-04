package providerhost

import (
	"fmt"

	"ptah.run/core/difftypes"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/internal/tableref"
)

type planningParents struct {
	tables    []featureplan.Table
	names     map[objectidentity.Key]string
	positions map[objectidentity.Key]int
	changed   map[objectidentity.Key]bool
	builder   objectidentity.Builder
	assess    bool
}

func newPlanningParents(builder objectidentity.Builder, assess bool) *planningParents {
	return &planningParents{builder: builder, assess: assess, names: make(map[objectidentity.Key]string),
		positions: make(map[objectidentity.Key]int), changed: make(map[objectidentity.Key]bool)}
}

func (p *planningParents) add(table featureplan.Table, name string) error {
	key := table.Subject.Key()
	ref, valid := tableref.Parse(name)
	if !valid || p.builder.TableParts(ref.Schema, ref.Name).Key() != key {
		return fmt.Errorf("%w: provider planning table name disagrees with its capture", schemaext.ErrInvalidValue)
	}
	if table.Action != "" {
		if p.changed[key] {
			return fmt.Errorf("%w: duplicate provider planning parent change", schemaext.ErrInvalidValue)
		}
		p.changed[key] = true
		kinds, err := table.CapturedKinds()
		if err != nil {
			return err
		}
		// With no registered parent model and no captured native state, a common
		// operation has no owner to assess. Registered owners still see empty state.
		if !p.assess && len(kinds) == 0 {
			table.Action = ""
		}
	}
	if index, found := p.positions[key]; found {
		if !p.changed[key] {
			return fmt.Errorf("%w: duplicate prepared planning table", schemaext.ErrInvalidValue)
		}
		p.tables[index] = table.Clone()
	} else {
		p.positions[key] = len(p.tables)
		p.tables = append(p.tables, table.Clone())
	}
	p.names[key] = name
	return nil
}

// alterForIndexes gives the owner of a table whose indexes change, and whose
// table the plan otherwise leaves alone, an alter action on it. The owner
// compares the declared indexes with the ones the table holds, as it does for
// any table it alters, so the change reaches it rather than being dropped. An
// index of a table the plan neither creates, prepares nor removes is refused.
func (p *planningParents) alterForIndexes(builder objectidentity.Builder, diff *difftypes.SchemaDiff) error {
	tables := make([]string, 0, len(diff.IndexesAdded)+len(diff.IndexesRemoved)+len(diff.IndexesRenamed))
	for _, change := range diff.IndexesAdded {
		tables = append(tables, change.TableName)
	}
	for _, ref := range diff.IndexesRemoved {
		tables = append(tables, ref.TableName)
	}
	for _, rename := range diff.IndexesRenamed {
		tables = append(tables, rename.TableName)
	}
	for _, name := range tables {
		key, err := tableKey(builder, name)
		if err != nil {
			return err
		}
		if p.changed[key] {
			continue
		}
		position, found := p.positions[key]
		if !found {
			return fmt.Errorf("%w: an index change names table %q, which the provider plan does not hold", schemaext.ErrInvalidValue, name)
		}
		p.tables[position].Action = featureplan.AlterTable
		p.changed[key] = true
	}
	return nil
}
