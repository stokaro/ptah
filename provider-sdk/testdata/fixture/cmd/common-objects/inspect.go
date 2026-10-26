package main

import (
	"context"
	"slices"
	"strings"

	"example.com/ptah-fixture-provider/backend"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

// Inspect reports the tables as the base fixture does, and, to a host that
// lists the common-object wire, the database's views and the roles, grants
// and comments the program keeps. Every kind it carries is read completely.
// A comment whose object the read does not hold is left out, since the base
// executor drops a table without telling the file.
func (p *commonObjects) Inspect(ctx context.Context, request *protocol.InspectRequest) (*protocol.Schema, error) {
	snapshot, err := p.Provider.Inspect(ctx, request)
	if err != nil || !slices.Contains(provider.HostFeatures(ctx), provider.CommonObjectsFeature) {
		return snapshot, err
	}
	observed := &protocol.ObservedCommonObjects{}
	for _, kind := range commonKinds {
		observed.Coverage = append(observed.Coverage, &protocol.CommonObjectCoverage{Kind: kind, Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}})
	}
	err = p.WithSession(ctx, request.GetSessionId(), "", func(access backend.Access) error {
		views, err := readViews(ctx, access)
		if err != nil {
			return err
		}
		held, err := loadState(access.Path)
		if err != nil {
			return err
		}
		observed.Views = views
		for _, role := range held.Roles {
			observed.Roles = append(observed.Roles, &protocol.ObservedRole{Name: role.Name, Login: role.Login, Group: role.Group,
				MemberOf: slices.Clone(role.MemberOf), Password: protocol.ObservedRole_PASSWORD_NONE})
		}
		for _, grant := range held.Grants {
			observed.Grants = append(observed.Grants, &protocol.ObservedGrant{Role: grant.Role, Privilege: grant.Privilege,
				Target: &protocol.Reference{Engine: "fixture", Kind: grant.Kind, Name: grant.Name}, WithGrantOption: grant.WithOption, Columns: slices.Clone(grant.Columns)})
		}
		for _, comment := range held.Comments {
			if object := commentedObject(snapshot, views, held, comment); object != nil {
				observed.Comments = append(observed.Comments, &protocol.ObjectComment{Object: object, Text: comment.Text})
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	snapshot.CommonObjects = observed
	return snapshot, nil
}

// readViews reads each view and the query SQLite keeps for it, its spaces
// folded as PrepareCommonObjects folds a declared one.
func readViews(ctx context.Context, access backend.Access) ([]*protocol.ObservedView, error) {
	rows, err := access.Querier.QueryContext(ctx, "SELECT name, sql FROM sqlite_schema WHERE type = 'view' ORDER BY name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var views []*protocol.ObservedView
	for rows.Next() {
		var name, definition string
		if err := rows.Scan(&name, &definition); err != nil {
			return nil, err
		}
		_, body, _ := strings.Cut(definition, " AS ")
		views = append(views, &protocol.ObservedView{Identity: &protocol.Reference{Engine: "fixture", Kind: "view", Name: name}, Body: foldSpaces(body)})
	}
	return views, rows.Err()
}

// foldSpaces writes a query with its runs of white space as one space, which
// is how the program compares queries.
func foldSpaces(query string) string { return strings.Join(strings.Fields(query), " ") }

// commentedObject is the reference of the object a kept comment belongs to,
// or nil when the read does not hold it.
func commentedObject(snapshot *protocol.Schema, views []*protocol.ObservedView, held state, comment stateComment) *protocol.Reference {
	tableHeld := func(name string) (*protocol.Table, bool) {
		i := slices.IndexFunc(snapshot.GetTables(), func(table *protocol.Table) bool { return table.GetIdentity().GetName() == name })
		if i < 0 {
			return nil, false
		}
		return snapshot.GetTables()[i], true
	}
	switch comment.Kind {
	case "table":
		if _, found := tableHeld(comment.Name); found {
			return &protocol.Reference{Engine: "fixture", Kind: "table", Name: comment.Name}
		}
	case "column":
		if table, found := tableHeld(comment.Table); found && slices.ContainsFunc(table.GetColumns(), func(column *protocol.Column) bool { return column.GetName() == comment.Name }) {
			return &protocol.Reference{Engine: "fixture", Kind: "column", Path: []string{comment.Table}, Name: comment.Name}
		}
	case "view":
		if slices.ContainsFunc(views, func(view *protocol.ObservedView) bool { return view.GetIdentity().GetName() == comment.Name }) {
			return &protocol.Reference{Engine: "fixture", Kind: "view", Name: comment.Name}
		}
	case "role":
		if held.role(comment.Name) >= 0 {
			return &protocol.Reference{Engine: "fixture", Kind: "role", Name: comment.Name}
		}
	}
	return nil
}
