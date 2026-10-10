// Package pgeffects describes what a PostgreSQL-family common statement does
// to the relations and columns it names: which ones it creates, changes or
// drops. Feature owners order their operations against these effects, both in
// a migration plan and in a whole-schema render. A statement it does not know
// keeps an unknown footprint; nothing here is invented.
package pgeffects

import (
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
)

// Sequence returns the effects of each statement in order. A statement can
// name an object an earlier one created -- CREATE OR REPLACE of a view the
// sequence created, an ALTER after a CREATE -- which a reading of one
// statement cannot tell; the object's history in the sequence can, so a
// second creation reads as a change. A write after a drop that is not a
// creation keeps an unknown footprint rather than an invented one.
func Sequence(builder objectidentity.Builder, nodes []ast.Node) [][]plangraph.Effect {
	writes := make(map[objectidentity.Key]plangraph.Action)
	result := make([][]plangraph.Effect, len(nodes))
	for i, node := range nodes {
		result[i] = lifecycle(writes, Statement(builder, node))
	}
	return result
}

// Statement describes the relations and columns one statement creates,
// changes or drops, read on its own: tables, the columns an ALTER TABLE adds,
// views and materialized views. Any other statement keeps an unknown
// footprint, and the result is nil; so does an object with no name, which
// names nothing an owner could order itself against.
func Statement(builder objectidentity.Builder, node ast.Node) []plangraph.Effect {
	effects := statement(builder, node)
	if slices.ContainsFunc(effects, func(effect plangraph.Effect) bool { return effect.Subject.Name.Normalized == "" }) {
		return nil
	}
	return effects
}

func statement(builder objectidentity.Builder, node ast.Node) []plangraph.Effect {
	switch typed := node.(type) {
	case *ast.CreateTableNode:
		return []plangraph.Effect{{Subject: builder.Table(typed.Name), Action: plangraph.Create}}
	case *ast.DropTableNode:
		names := typed.Names
		if len(names) == 0 {
			names = []string{typed.Name}
		}
		var effects []plangraph.Effect
		for _, name := range names {
			effects = append(effects, plangraph.Effect{Subject: builder.Table(name), Action: plangraph.Drop})
		}
		return dedupe(effects)
	case *ast.AlterTableNode:
		effects := []plangraph.Effect{{Subject: builder.Table(typed.Name), Action: plangraph.Alter}}
		for _, operation := range typed.Operations {
			if add, ok := operation.(*ast.AddColumnOperation); ok && add.Column != nil {
				effects = append(effects, plangraph.Effect{Subject: builder.Column(typed.Name, add.Column.Name), Action: plangraph.Create})
			}
		}
		return dedupe(effects)
	case *ast.CreateViewNode:
		action := plangraph.Create
		if typed.Replace {
			action = plangraph.Alter
		}
		return []plangraph.Effect{{Subject: relation(builder, objectidentity.KindView, typed.Name), Action: action}}
	case *ast.CreateMaterializedViewNode:
		return []plangraph.Effect{{Subject: relation(builder, objectidentity.KindMatView, typed.Name), Action: plangraph.Create}}
	default:
		return nil
	}
}

// CreatesView reports a statement that creates a view or a materialized view,
// the relations that may read a feature object.
func CreatesView(effects []plangraph.Effect) bool {
	return slices.ContainsFunc(effects, func(effect plangraph.Effect) bool {
		return effect.Action == plangraph.Create && (effect.Subject.Kind == objectidentity.KindView || effect.Subject.Kind == objectidentity.KindMatView)
	})
}

func lifecycle(writes map[objectidentity.Key]plangraph.Action, effects []plangraph.Effect) []plangraph.Effect {
	if effects == nil {
		return nil
	}
	result := make([]plangraph.Effect, 0, len(effects))
	for _, effect := range effects {
		key := effect.Subject.Key()
		previous := writes[key]
		switch {
		case previous == plangraph.Drop && effect.Action != plangraph.Create:
			continue
		case effect.Action == plangraph.Create && previous != "" && previous != plangraph.Drop:
			effect.Action = plangraph.Alter
		}
		writes[key] = effect.Action
		result = append(result, effect)
	}
	return result
}

func relation(builder objectidentity.Builder, kind objectidentity.Kind, name string) objectidentity.ID {
	table := builder.Table(name)
	table.Kind = kind
	return table
}

func dedupe(effects []plangraph.Effect) []plangraph.Effect {
	seen := make(map[objectidentity.Key]bool, len(effects))
	return slices.DeleteFunc(effects, func(effect plangraph.Effect) bool {
		key := effect.Subject.Key()
		if seen[key] {
			return true
		}
		seen[key] = true
		return false
	})
}
