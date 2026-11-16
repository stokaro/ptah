package main

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"net/url"
	"os"
	"slices"
	"strings"

	"example.com/ptah-fixture-provider/backend"
	"example.com/ptah-fixture-provider/devdb"

	protocol "ptah.run/provider-sdk/protocol/experimental"
)

// With FIXTURE_NAMESPACE_VIEWS=true the engine carries views too. The program
// keeps each view in a file beside the database rather than in SQLite, whose
// views cannot be named by schema, and writes it as one statement its executor
// reads: `FIXTURE CREATE VIEW "app"."v" AS SELECT id FROM identity.u;`. The
// executor refuses a view whose query names a relation the database does not
// hold, as a server with schemas does, and the removal of a schema that still
// holds a view.

// storedView is a view the program keeps.
type storedView struct {
	Schema string `json:"schema"`
	Name   string `json:"name"`
	Body   string `json:"body"`
}

// viewsPath is the file beside a database that holds its views.
func viewsPath(database string) string { return database + ".views.json" }

func loadViews(database string) ([]storedView, error) {
	data, err := os.ReadFile(viewsPath(database)) // #nosec G304 -- the path is the session's own database file
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var views []storedView
	return views, json.Unmarshal(data, &views)
}

func saveViews(database string, views []storedView) error {
	slices.SortFunc(views, func(a, b storedView) int { return strings.Compare(a.Schema+"."+a.Name, b.Schema+"."+b.Name) })
	data, err := json.Marshal(views)
	if err != nil {
		return err
	}
	return os.WriteFile(viewsPath(database), data, 0o600)
}

// viewSchema is the schema a view's identity names: none for main, as a
// table's.
func viewSchema(schema string) string {
	if schema == defaultSchema {
		return ""
	}
	return schema
}

// qualifiedName spells a relation of schema as the program's statements do.
func qualifiedName(schema, name string) string {
	if schema == "" || schema == defaultSchema {
		return quote(name)
	}
	return quote(schema) + "." + quote(name)
}

// viewStatement writes a view change, and reports whether change is one.
func viewStatement(change *protocol.CommonObjectChange) (string, bool) {
	switch operation := change.GetOperation().(type) {
	case *protocol.CommonObjectChange_CreateView:
		view := operation.CreateView.GetView()
		return statementPrefix + "CREATE VIEW " + qualifiedName(view.GetSchema(), view.GetName()) + " AS " + view.GetBody() + ";", true
	case *protocol.CommonObjectChange_DropView:
		return statementPrefix + "DROP VIEW " + qualifiedName(operation.DropView.GetSchema(), operation.DropView.GetName()) + ";", true
	default:
		return "", false
	}
}

// readView reads a view statement the program wrote: whether it creates, and
// the view.
func readView(sql string) (create bool, view storedView, own bool) {
	text, found := strings.CutPrefix(strings.TrimSpace(sql), statementPrefix)
	if !found {
		return false, storedView{}, false
	}
	text = strings.TrimSuffix(text, ";")
	if rest, created := strings.CutPrefix(text, "CREATE VIEW "); created {
		name, body, _ := strings.Cut(rest, " AS ")
		view = splitRelation(name)
		view.Body = body
		return true, view, true
	}
	if name, dropped := strings.CutPrefix(text, "DROP VIEW "); dropped {
		return false, splitRelation(name), true
	}
	return false, storedView{}, false
}

// splitRelation reads a relation's name as a statement spells it, quoted or
// not: "app"."v", app.v, or v in main.
func splitRelation(spelled string) storedView {
	spelled = strings.TrimRight(strings.TrimSpace(spelled), ";),")
	schema, name, qualified := strings.Cut(spelled, ".")
	if !qualified {
		return storedView{Schema: defaultSchema, Name: unquote(schema)}
	}
	return storedView{Schema: unquote(schema), Name: unquote(name)}
}

// relationsNamed are the relations a query names after FROM or JOIN.
func relationsNamed(body string) []storedView {
	words := strings.FieldsFunc(body, func(r rune) bool { return r == ' ' || r == '\t' || r == '\n' || r == ',' || r == '(' })
	var named []storedView
	for i, word := range words[:max(len(words)-1, 0)] {
		if strings.EqualFold(word, "FROM") || strings.EqualFold(word, "JOIN") {
			named = append(named, splitRelation(words[i+1]))
		}
	}
	return named
}

// applyView creates or removes a view, and returns the reason it refuses one:
// a view of a schema the file does not hold, one created twice, one whose
// query names a relation the database does not hold, and the removal of a
// view it does not hold.
func applyView(ctx context.Context, access backend.Access, held []string, create bool, view storedView) (string, error) {
	views, err := loadViews(access.Path)
	if err != nil {
		return "", err
	}
	at := slices.IndexFunc(views, func(kept storedView) bool { return kept.Schema == view.Schema && kept.Name == view.Name })
	switch {
	case !create && at < 0:
		return "View not found: " + view.Schema + "." + view.Name, nil
	case !create:
		return "", saveViews(access.Path, slices.Delete(views, at, at+1))
	case view.Schema != defaultSchema && !slices.Contains(held, view.Schema):
		return "Schema not found: " + view.Schema, nil
	case at >= 0:
		return "View " + view.Schema + "." + view.Name + " already exists", nil
	}
	for _, relation := range relationsNamed(view.Body) {
		found, err := relationHeld(ctx, access, views, relation)
		if err != nil {
			return "", err
		}
		if !found {
			return "Relation not found: " + relation.Schema + "." + relation.Name, nil
		}
	}
	return "", saveViews(access.Path, append(views, view))
}

// relationHeld reports whether the database holds a table or a view of the
// relation's name.
func relationHeld(ctx context.Context, access backend.Access, views []storedView, relation storedView) (bool, error) {
	if slices.ContainsFunc(views, func(kept storedView) bool { return kept.Schema == relation.Schema && kept.Name == relation.Name }) {
		return true, nil
	}
	var count int
	err := access.Querier.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name = ?", storedTable(relation.Schema, relation.Name)).Scan(&count)
	return count != 0, err
}

// observedViews are the views of the schemas inScope selects, as a read
// reports them.
func (p *namespaces) observedViews(path string, inScope func(string) bool) ([]*protocol.ObservedView, error) {
	views, err := loadViews(path)
	if err != nil {
		return nil, err
	}
	var observed []*protocol.ObservedView
	for _, view := range views {
		if inScope(view.Schema) {
			observed = append(observed, &protocol.ObservedView{Identity: &protocol.Reference{Engine: p.engine, Kind: "view", Schema: viewSchema(view.Schema),
				Name: view.Name}, Body: view.Body})
		}
	}
	return observed, nil
}

// viewsIn are the views of the named schemas, and with no schema named every
// view.
func viewsIn(path string, scope []string) ([]storedView, error) {
	views, err := loadViews(path)
	if err != nil {
		return nil, err
	}
	return slices.DeleteFunc(views, func(view storedView) bool { return len(scope) != 0 && !slices.Contains(scope, view.Schema) }), nil
}

// dropViewsIn removes the views of the named schemas, and with no schema named
// every view.
func dropViewsIn(path string, scope []string) error {
	views, err := loadViews(path)
	if err != nil || len(views) == 0 {
		return err
	}
	return saveViews(path, slices.DeleteFunc(views, func(view storedView) bool { return len(scope) == 0 || slices.Contains(scope, view.Schema) }))
}

// ProvisionDevDatabase is the devdb program's, and records the database file
// of each lease, so the release removes the files the program keeps beside it.
func (p *namespaces) ProvisionDevDatabase(ctx context.Context, request *protocol.ProvisionDevRequest) (*protocol.DevDatabase, error) {
	database, err := p.Provider.ProvisionDevDatabase(ctx, request)
	if err != nil || database.GetLease() == "" {
		return database, err
	}
	address, err := url.Parse(database.GetUrl())
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.leases == nil {
		p.leases = make(map[string]string)
	}
	p.leases[database.GetLease()] = devdb.FilePath(address.Path)
	return database, nil
}

// ReleaseDevDatabase is the devdb program's, which removes the database file
// of a lease, and removes the files of its schemas and views as well.
func (p *namespaces) ReleaseDevDatabase(ctx context.Context, request *protocol.ReleaseDevRequest) (*protocol.Empty, error) {
	released, err := p.Provider.ReleaseDevDatabase(ctx, request)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	path, found := p.leases[request.GetLease()]
	delete(p.leases, request.GetLease())
	p.mu.Unlock()
	if !found {
		return released, nil
	}
	for _, kept := range []string{schemasPath(path), viewsPath(path)} {
		if err := os.Remove(kept); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return nil, err
		}
	}
	return released, nil
}
