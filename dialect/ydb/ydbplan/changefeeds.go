package ydbplan

import (
	"fmt"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/internal/ydbchangefeed"
)

// changefeedContribution exposes the stream limit and topic ordering as graph
// dependencies. Sorting names alone cannot keep an addition behind every drop.
func changefeedContribution(table string, records []schemaext.ChangeRecord, tableIndex int) (plangraph.Contribution[featureplan.Operation], []featureplan.ChangePlan, error) {
	plan := streamPlan{contribution: plangraph.Contribution[featureplan.Operation]{Owner: "ptah.run/ydb"}, table: table, tableIndex: tableIndex}
	records = slices.Clone(records)
	slices.SortFunc(records, func(a, b schemaext.ChangeRecord) int { return schemaext.CompareRefs(a.Subject, b.Subject) })
	for i, record := range records {
		change, ok := record.Value.(*ydbdiff.Changefeed)
		if !ok || change == nil || (change.Before == nil && change.After == nil) {
			return plangraph.Contribution[featureplan.Operation]{}, nil, refuseFact(table, "no valid changefeed operands to lower")
		}
		start := len(plan.contribution.Steps)
		plan.addChange(i, record.Subject, change)
		disposition := featureplan.ChangePlan{Subject: record.Subject, Kind: record.Value.Kind(), Strategy: "unchanged definition"}
		for _, step := range plan.contribution.Steps[start:] {
			disposition.Steps = append(disposition.Steps, step.ID)
		}
		if len(disposition.Steps) > 0 {
			disposition.Strategy = "apply changefeed operations"
		}
		plan.changes = append(plan.changes, disposition)
	}
	plan.contribution.Dependencies = append(plan.contribution.Dependencies, dependencies(plan.drops, plan.adds)...)
	plan.contribution.Dependencies = append(plan.contribution.Dependencies, dependencies(slices.Concat(plan.drops, plan.adds), plan.topics)...)
	return plan.contribution, plan.changes, nil
}

type streamPlan struct {
	contribution        plangraph.Contribution[featureplan.Operation]
	table               string
	tableIndex          int
	changes             []featureplan.ChangePlan
	drops, adds, topics []plangraph.StepID
}

func (p *streamPlan) addChange(index int, subject objectidentity.ID, change *ydbdiff.Changefeed) {
	switch {
	case change.Before == nil:
		p.adds = append(p.adds, p.step(index, subject, plangraph.Create, &ydbast.AddChangefeed{Changefeed: change.After.Spec.Clone()}, ""))
	case change.After == nil:
		current := change.Before.Spec
		p.drops = append(p.drops, p.step(index, subject, plangraph.Drop, &ydbast.DropChangefeed{Name: current.Name}, ydbchangefeed.DroppedNote(p.table, current)))
	case ydbchangefeed.Recreated(change.After.Spec, change.Before.Spec):
		current, desired := change.Before.Spec, change.After.Spec
		p.drops = append(p.drops, p.step(index, subject, plangraph.Drop, &ydbast.DropChangefeed{Name: current.Name}, ydbchangefeed.RecreatedNote(p.table, current)))
		p.adds = append(p.adds, p.step(index, subject, plangraph.Create, &ydbast.AddChangefeed{Changefeed: desired.Clone()}, ""))
	case ydbchangefeed.TopicChanged(change.After.Spec, change.Before.Spec):
		current, desired := change.Before.Spec, change.After.Spec
		var note string
		if _, restarted := ydbchangefeed.TopicStatements(p.table, desired, current); len(restarted) > 0 {
			note = ydbchangefeed.RestartedConsumersNote(p.table, current.Name, restarted)
		}
		p.topics = append(p.topics, p.step(index, subject, plangraph.Alter, &ydbast.AlterChangefeedTopic{Changefeed: desired.Clone(), Previous: current.Clone()}, note))
	}
}

type streamOperation interface {
	ast.ExtensionPayload
	schemaext.EffectSource
}

func (p *streamPlan) step(index int, subject objectidentity.ID, action plangraph.Action, payload streamOperation, note string) plangraph.StepID {
	id := plangraph.StepID{Owner: p.contribution.Owner, Name: fmt.Sprintf("changefeed/%06d/%06d/%s", p.tableIndex, index, action)}
	var notes []string
	if note != "" {
		notes = append(notes, note)
	}
	parent := objectidentity.ID{Kind: objectidentity.KindTable, Catalog: subject.Catalog, Schema: subject.Schema, Name: subject.Parent}
	p.contribution.Steps = append(p.contribution.Steps, plangraph.Step[featureplan.Operation]{
		ID: id, Payload: featureplan.Operation{Role: ast.AlterExtension, Parent: parent, Payload: payload, Notes: notes}, Transaction: plangraph.TransactionForbidden,
		Impact:  payload.Effect(),
		Effects: []plangraph.Effect{{Subject: subject, Action: action}, {Subject: parent, Action: plangraph.Read}},
	})
	return id
}

func dependencies(before, after []plangraph.StepID) []plangraph.Dependency {
	var edges []plangraph.Dependency
	for _, first := range before {
		for _, last := range after {
			edges = append(edges, plangraph.Dependency{Before: first, After: last})
		}
	}
	return edges
}
