package providerhost

import (
	"fmt"

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
