package postgresreplay

import (
	"encoding/json"
	"fmt"
	"slices"
	"sync"

	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/lexer"
)

// This file holds what the server writes when a statement writes a relation
// outside the namespaces it owns: the relation a view writes through, the
// relations a rule's action writes, the relations a trigger's arguments name,
// and the tables a foreign key, a partition or an inheritance child carries
// the write on to. Measured on PostgreSQL 18, each of these carries a write
// into pg_catalog past a guard that judges a relation when it is created:
//
//	CREATE VIEW inner_v AS SELECT rolname, rolcreatedb FROM harmless;
//	CREATE VIEW outer_v AS SELECT * FROM inner_v;
//	CREATE OR REPLACE VIEW inner_v AS SELECT rolname, rolcreatedb FROM pg_authid;
//	UPDATE outer_v SET rolcreatedb = true;
//
// So a write is judged when it is written, over the relations as they stand
// then ([relationGraph.writeReach]).

// relationKey is a relation's schema and name as the server stores them: an
// unquoted identifier folded to lower case, a quoted one as written
// ([foldedNameAt]).
type relationKey struct {
	schema, name string
}

// relationRef is what a view, a rule, a trigger or a foreign key refers to,
// in one of three forms. A view's query, a rule's action and a foreign key
// are bound to the relation the server finds when they are created, and stay
// bound to it when it is renamed or replaced: node. A relation of a namespace
// the server owns needs no node: protected. A trigger's argument is text the
// trigger function reads when it runs, so its name is looked up each time a
// write is judged, and so is a name the guard could not bind: name.
type relationRef struct {
	node      *relationNode
	protected string
	name      []string
}

// relationNode is one relation outside the namespaces the server owns, as the
// dev session held it or a replayed statement created it.
type relationNode struct {
	key relationKey
	// dropped is set when the relation is dropped. A reference bound to it
	// reaches nothing more, as the server drops what depends on it or refuses
	// the drop.
	dropped bool
	// viewBase is the one relation an automatically updatable view writes
	// through, and is nil for a relation that is no such view
	// ([shapedViewBase]); viewNames is every relation such a view's query
	// names, read when viewBase is nil.
	viewBase  *relationRef
	viewNames []relationRef
	// rules are the relations each rule's action writes, by rule name.
	rules map[string][]relationRef
	// triggers are the names each trigger's arguments hold, by trigger name.
	triggers map[string][]relationRef
	// children are the tables a write carries on to: the ones whose foreign
	// key to this table has a referential action, its partitions and its
	// inheritance children.
	children []relationRef
}

// edges returns every reference a write to n follows, in a fixed order so
// that a refusal names the same namespace on every run.
func (n *relationNode) edges() []relationRef {
	var edges []relationRef
	if n.viewBase != nil {
		edges = append(edges, *n.viewBase)
	} else {
		edges = append(edges, n.viewNames...)
	}
	for _, name := range sortedNames(n.rules) {
		edges = append(edges, n.rules[name]...)
	}
	for _, name := range sortedNames(n.triggers) {
		edges = append(edges, n.triggers[name]...)
	}
	return append(edges, n.children...)
}

func sortedNames(byName map[string][]relationRef) []string {
	names := make([]string, 0, len(byName))
	for name := range byName {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// relationGraph is every relation outside the namespaces the server owns that
// the guard knows, with what a write to each reaches. It holds the relations
// of the dev session's search path and the views and rules of the dev
// database when [guard.ValidateSession] read it ([loadRelationGraph]), and
// what each statement the guard accepted after that created, replaced,
// renamed, moved or dropped ([relationGraph.record]). The guard and every
// copy of it share one value, so a statement is judged with every change the
// statements validated before it made. The methods take nil as a graph that
// holds nothing.
type relationGraph struct {
	mu    sync.Mutex
	byKey map[relationKey]*relationNode
	// session is set once a dev session was read. A guard that read none
	// cannot say which schema a name without one finds, and matches such a
	// name in every schema.
	session bool
	// searchPath is the session's search_path as the server reads it, with
	// $user replaced by the session's user, including schemas that do not
	// exist yet; see [relationGraph.lookupPath].
	searchPath []string
	// schemas are the schemas the dev database holds: the ones the session
	// read, and the ones the replay created, less the ones it dropped.
	schemas map[string]bool
}

func newRelationGraph() *relationGraph {
	return &relationGraph{byKey: make(map[relationKey]*relationNode), schemas: make(map[string]bool)}
}

// live returns the relation at key, or nil when there is none or it was
// dropped.
func (g *relationGraph) live(key relationKey) *relationNode {
	node := g.byKey[key]
	if node == nil || node.dropped {
		return nil
	}
	return node
}

// lookupPath is the order the server looks a relation named without a schema
// up in: the session's temporary schema first unless the search path lists
// it, then the schemas of the path. pg_catalog, which the server searches
// before the path unless the path lists it, is read before the graph is
// ([protection.unqualifiedNamespace]).
func (g *relationGraph) lookupPath() []string {
	if slices.Contains(g.searchPath, "pg_temp") {
		return g.searchPath
	}
	return append([]string{"pg_temp"}, g.searchPath...)
}

// resolve returns the relation the dotted folded name parts names, as the
// server finds it now, or nil when the guard knows none. A name of three
// parts is a relation of the dev database only when the first part is the
// database's name.
func (g *relationGraph) resolve(p protection, parts []string) *relationNode {
	switch len(parts) {
	case 1:
		if !g.session {
			return nil
		}
		for _, schema := range g.lookupPath() {
			if node := g.live(relationKey{schema: schema, name: parts[0]}); node != nil {
				return node
			}
		}
		return nil
	case 2:
		return g.live(relationKey{schema: parts[0], name: parts[1]})
	case 3:
		if p.database != "" && parts[0] != p.database {
			return nil
		}
		return g.live(relationKey{schema: parts[1], name: parts[2]})
	}
	return nil
}

// sameName returns the relations named name in any schema, for a guard that
// read no session and so cannot say which one the server finds.
func (g *relationGraph) sameName(name string) []*relationNode {
	var nodes []*relationNode
	for _, key := range g.sortedKeys() {
		if key.name == name && !g.byKey[key].dropped {
			nodes = append(nodes, g.byKey[key])
		}
	}
	return nodes
}

func (g *relationGraph) sortedKeys() []relationKey {
	keys := make([]relationKey, 0, len(g.byKey))
	for key := range g.byKey {
		keys = append(keys, key)
	}
	slices.SortFunc(keys, func(a, b relationKey) int {
		if a.schema != b.schema {
			if a.schema < b.schema {
				return -1
			}
			return 1
		}
		switch {
		case a.name < b.name:
			return -1
		case a.name > b.name:
			return 1
		}
		return 0
	})
	return keys
}

// reachFrom returns the namespace the server owns that a write to start
// reaches, following every edge of every relation it reaches with a visited
// set, or "". A name an edge holds is read as a name of any class
// ([protection.protectedNameReach]) and looked up now
// ([relationGraph.candidates]).
func (g *relationGraph) reachFrom(p protection, start *relationNode) string {
	scan := p.scanning()
	visited := make(map[*relationNode]bool)
	pending := []*relationNode{start}
	for len(pending) > 0 {
		node := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if node == nil || node.dropped || visited[node] {
			continue
		}
		visited[node] = true
		for _, ref := range node.edges() {
			switch {
			case ref.protected != "":
				return ref.protected
			case ref.node != nil:
				pending = append(pending, ref.node)
			case ref.name != nil:
				if namespace := scan.protectedNameReach(ref.name); namespace != "" {
					return namespace
				}
				pending = append(pending, g.candidates(scan, ref.name)...)
			}
		}
	}
	return ""
}

// candidates returns the relations a dotted name of any class can name: each
// of its prefixes, read as a relation and the columns after it, as
// `descs.objoid` names the relation descs.
func (g *relationGraph) candidates(p protection, parts []string) []*relationNode {
	var nodes []*relationNode
	for end := 1; end <= len(parts); end++ {
		if node := g.resolve(p, parts[:end]); node != nil {
			nodes = append(nodes, node)
		}
		if end == 1 && !g.session {
			nodes = append(nodes, g.sameName(parts[0])...)
		}
	}
	return nodes
}

// writeReach returns the namespace the server owns that a write to the
// relation the dotted folded name parts names reaches through the relations
// the graph holds, or "", and whether the guard read no session and so took
// a name without a schema to name every relation of that name.
func (g *relationGraph) writeReach(p protection, parts []string) (string, bool) {
	if g == nil || len(parts) == 0 {
		return "", false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if node := g.resolve(p, parts); node != nil {
		return g.reachFrom(p, node), false
	}
	if len(parts) == 1 && !g.session {
		for _, node := range g.sameName(parts[0]) {
			if namespace := g.reachFrom(p, node); namespace != "" {
				return namespace, true
			}
		}
	}
	return "", false
}

// nameReach returns the namespace the server owns that a write reaches
// through a relation a dotted folded name of any class names, such as a name
// in a trigger's argument, or "".
func (g *relationGraph) nameReach(p protection, parts []string) string {
	if g == nil || len(parts) == 0 {
		return ""
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, node := range g.candidates(p.scanning(), parts) {
		if namespace := g.reachFrom(p, node); namespace != "" {
			return namespace
		}
	}
	return ""
}

// knowsSchema reports whether the dev database holds schema, as the session
// read it and the replay changed it, and whether the guard read a session at
// all.
func (g *relationGraph) knowsSchema(schema string) (known, read bool) {
	if g == nil {
		return false, false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.schemas[schema], g.session
}

// record applies to the graph what an accepted statement changes about the
// relations a write reaches through ([relationGraph.apply]).
func (g *relationGraph) record(p protection, tokens []lexer.Token) {
	if g == nil || len(tokens) == 0 {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.apply(p.scanning(), tokens)
}

// loadRelationGraph returns the graph of the dev session: the relations of
// each namespace of its search path the server does not own, relations, and
// the rules of the dev database, encodedRules, the fifth column of
// [sessionCatalogQuery] as pg_get_ruledef writes each rule, read as the
// replay's CREATE RULE is ([relationGraph.createRule]). A rule of a relation
// in a namespace the server owns is left out: a write to that relation is
// refused for its namespace. Every relation a rule is on is in the graph
// before any rule is read, so a view over a view is bound to it whichever
// order the server lists them in.
func loadRelationGraph(p protection, path sessionPath, relations map[string][]string, encodedRules string) (*relationGraph, error) {
	var rows [][3]string
	if err := json.Unmarshal([]byte(encodedRules), &rows); err != nil {
		return nil, fmt.Errorf("read the dev session's rules: %w", err)
	}
	g := newRelationGraph()
	g.session = true
	g.searchPath = path.searchPath()
	for _, schema := range path.Schemas {
		g.schemas[schema] = true
	}
	for schema, names := range relations {
		for _, name := range names {
			key := relationKey{schema: schema, name: name}
			if g.byKey[key] == nil {
				g.byKey[key] = &relationNode{key: key}
			}
		}
	}
	rules := make([][]lexer.Token, 0, len(rows))
	for _, row := range rows {
		if p.isProtectedPostgresNamespace(row[0]) {
			continue
		}
		key := relationKey{schema: row[0], name: row[1]}
		if g.byKey[key] == nil {
			g.byKey[key] = &relationNode{key: key}
		}
		rules = append(rules, replay.Tokens(row[2], lexerOptions()))
	}
	for _, rule := range rules {
		g.apply(p, rule)
	}
	return g, nil
}
