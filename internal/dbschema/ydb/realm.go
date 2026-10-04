package ydb

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"

	"ptah.run/core/platform"
	"ptah.run/internal/dbreset"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/ydburl"
)

// The writer's dev database operations. A dev database on YDB is the root a
// connection treats as its database: a dev realm Ptah created for the run, or
// a whole database on a server the run owns (see ydburl.RealmParameter). A run
// claims it only when it is empty, resets it before and after it uses it, and
// locks and compares it by [Writer.RealmIdentity].

// rootEntry is one object under the writer's root, as a reset of the root
// finds it: where it is, what it is, and the step that drops it, which is
// empty for an object Ptah has no statement to drop.
type rootEntry struct {
	object dbreset.Object
	step   treeStep
}

// ResetObjects lists every object under the writer's root that
// [Writer.DropDatabaseRealm] would drop or refuse to: tables, views, topics,
// transfers, async replications and the directories that hold them, contents
// before the directory, and any other object under its own kind, in the order
// a depth-first walk with each directory's entries sorted by name meets them.
// It leaves out what the reset leaves alone: the server's dot-directories, and
// at the root of a database the coordination node Ptah's locks live on and the
// directory that holds the dev realms. The scope is ignored: a YDB reset
// empties the root.
//
// A dev database is claimed only when the list is empty, so the check that
// refuses one and the reset that would empty it read the same list.
func (w *Writer) ResetObjects(ctx context.Context, _ dbreset.Scope) ([]dbreset.Object, error) {
	entries, err := w.rootEntries(ctx)
	if err != nil {
		return nil, err
	}
	objects := make([]dbreset.Object, 0, len(entries))
	for _, entry := range entries {
		objects = append(objects, entry.object)
	}
	return objects, nil
}

// DropDatabaseRealm empties the writer's root: it drops every transfer and
// async replication under it, then every table, column table, view and topic,
// and removes every directory below it, deepest first. The root itself stays.
// What [Writer.ResetObjects] leaves out is left alone.
//
// An object Ptah has no statement to drop, such as a coordination node, stops
// it before
// anything is dropped, with the object named: a reset that dropped the rest
// and left it would hand the next run a dev database that is not empty.
func (w *Writer) DropDatabaseRealm(ctx context.Context) error {
	entries, err := w.rootEntries(ctx)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.step == (treeStep{}) {
			return fmt.Errorf("ydb: %s holds %s, which Ptah has no statement to drop; nothing was dropped",
				w.root, describeObject(entry.object))
		}
	}
	steps := make([]treeStep, 0, len(entries))
	for _, entry := range entries {
		steps = append(steps, entry.step)
	}
	slices.SortStableFunc(steps, func(a, b treeStep) int { return cmp.Compare(a.rank, b.rank) })
	for _, step := range steps {
		if err := w.runTreeStep(ctx, step); err != nil {
			return err
		}
	}
	return nil
}

// DropAllTablesKeeping is [Writer.DropDatabaseRealm] for a reset that keeps a
// dev database's environment. A YDB database has no extension, default
// privilege or database-scoped object a reset keeps, so a kept environment
// that names one is refused rather than dropped.
func (w *Writer) DropAllTablesKeeping(ctx context.Context, kept dbreset.Kept) error {
	if len(kept.Extensions) > 0 || len(kept.Schemas) > 0 || len(kept.DefaultPrivileges.Rows) > 0 ||
		len(kept.Artifacts) > 0 || kept.StartingPoint != nil {
		return errors.New("ydb: a YDB dev database has no environment a reset keeps, and this one names some")
	}
	return w.DropDatabaseRealm(ctx)
}

// rootEntries walks the writer's root for a reset.
func (w *Writer) rootEntries(ctx context.Context) ([]rootEntry, error) {
	if w.scheme == nil {
		return nil, errors.New("no YDB scheme connection")
	}
	var entries []rootEntry
	if err := w.walkTree(ctx, tree{root: w.root, leaveAlone: w.leftAlone}, "", &entries); err != nil {
		return nil, err
	}
	return entries, nil
}

// tree is a directory tree a walk reads: its absolute root, the path the
// writer's runner resolves a statement's names against relative to it, and
// what the walk leaves where it is.
type tree struct {
	root string
	// base is written in front of each object's path in the statement that
	// drops it: empty when the runner resolves names against root, as it does
	// for a connection whose root it is.
	base string
	// leaveAlone reports an entry of the directory dir, relative to root, that
	// the walk skips; nil skips nothing.
	leaveAlone func(dir string, entry *Ydb_Scheme.Entry) bool
}

// walkTree appends what the directory dir, relative to t's root, holds, each
// directory after its contents.
func (w *Writer) walkTree(ctx context.Context, t tree, dir string, entries *[]rootEntry) error {
	listed, err := w.scheme.ListDirectory(ctx, path.Join(t.root, dir))
	if err != nil {
		return err
	}
	slices.SortFunc(listed, func(a, b *Ydb_Scheme.Entry) int { return strings.Compare(a.GetName(), b.GetName()) })
	for _, entry := range listed {
		if t.leaveAlone != nil && t.leaveAlone(dir, entry) {
			continue
		}
		name := entry.GetName()
		child := path.Join(dir, name)
		object := dbreset.Object{Kind: objectKind(entry.GetType()), Schema: dir, Name: name}
		if entry.GetType() == Ydb_Scheme.Entry_DIRECTORY {
			if err := w.walkTree(ctx, t, child, entries); err != nil {
				return err
			}
			*entries = append(*entries, rootEntry{object: object, step: treeStep{directory: path.Join(t.root, child),
				rank: teardownRank(Ydb_Scheme.Entry_DIRECTORY)}})
			continue
		}
		var step treeStep
		if statement, droppable := treeStatements[entry.GetType()]; droppable {
			step = treeStep{statement: fmt.Sprintf(statement, sqlident.Quote(platform.YDB, path.Join(t.base, child))),
				rank: teardownRank(entry.GetType())}
		}
		*entries = append(*entries, rootEntry{object: object, step: step})
	}
	return nil
}

// leftAlone reports whether a reset of the root leaves entry, in the directory
// dir relative to the root, where it is: a name that starts with a dot belongs
// to the server, and at the root of a database the coordination node Ptah's
// locks live on and the directory of the dev realms belong to other runs.
func (w *Writer) leftAlone(dir string, entry *Ydb_Scheme.Entry) bool {
	name := entry.GetName()
	switch {
	case strings.HasPrefix(name, "."):
		return true
	case dir != "" || w.root != w.database:
		return false
	case entry.GetType() == Ydb_Scheme.Entry_DIRECTORY && name == ydburl.RealmDirectory:
		return true
	default:
		return entry.GetType() == Ydb_Scheme.Entry_COORDINATION_NODE && name == LockNode
	}
}

// objectKind names a scheme entry type in lower case words, as a refusal
// names the object: "table", "column table", "coordination node".
func objectKind(entryType Ydb_Scheme.Entry_Type) string {
	if name, known := Ydb_Scheme.Entry_Type_name[int32(entryType)]; known {
		return strings.ToLower(strings.ReplaceAll(name, "_", " "))
	}
	return fmt.Sprintf("scheme entry of type %d", int32(entryType))
}

// describeObject names object with its directory, as an error reads it.
func describeObject(object dbreset.Object) string {
	if object.Schema == "" {
		return fmt.Sprintf("%s %q", object.Kind, object.Name)
	}
	return fmt.Sprintf("%s %q in directory %q", object.Kind, object.Name, object.Schema)
}

// MakeDirectory creates the directory dir, relative to the root, with every
// directory above it that does not exist. Creating one that exists succeeds.
//
// The scheme service asks for a right of its own: measured on 26.2.1.14, an
// account granted ydb.granular.create_table on the database creates a table
// in a directory that does not exist yet, and is refused the directory alone
// (`Access denied`) until it holds ydb.granular.create_directory. A refusal
// names that right.
func (w *Writer) MakeDirectory(ctx context.Context, dir string) error {
	if w.scheme == nil {
		return errors.New("no YDB scheme connection")
	}
	absolute := path.Join(w.root, dir)
	err := w.scheme.MakeDirectory(ctx, absolute)
	switch {
	case err == nil:
		return nil
	case isUnauthorized(err):
		return fmt.Errorf("ydb: create directory %s: %w; creating a directory needs the "+
			"ydb.granular.create_directory right on the database", absolute, err)
	default:
		return fmt.Errorf("ydb: create directory %s: %w", absolute, err)
	}
}

// RemoveRealm removes the dev realm realm, with everything in it, and then
// the directory of the dev realms when no other realm is left in it. It is
// the end of a realm a run created for itself, so every object in the realm
// is the run's, a name that starts with a dot included. The whole realm is
// read and checked first: an object Ptah has no statement to drop stops it
// before anything is dropped, with the object named.
//
// The writer must be the database's, not a realm's. A realm that does not
// exist is removed already, and that is not an error.
func (w *Writer) RemoveRealm(ctx context.Context, realm string) error {
	if w.scheme == nil {
		return errors.New("no YDB scheme connection")
	}
	if w.root != w.database {
		return fmt.Errorf("ydb: a realm is removed through its database, not through %s", w.root)
	}
	if err := ydburl.CheckRealm(realm); err != nil {
		return err
	}
	parent := path.Join(w.database, ydburl.RealmDirectory)
	listed, err := w.scheme.ListDirectory(ctx, parent)
	if isSchemeError(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if !slices.ContainsFunc(listed, func(entry *Ydb_Scheme.Entry) bool { return entry.GetName() == realm }) {
		return nil
	}
	relative := path.Join(ydburl.RealmDirectory, realm)
	absolute := path.Join(w.database, relative)
	var entries []rootEntry
	if err := w.walkTree(ctx, tree{root: absolute, base: relative}, "", &entries); err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.step == (treeStep{}) {
			return fmt.Errorf("ydb: the dev realm %s holds %s, which Ptah has no statement to drop; nothing was dropped",
				absolute, describeObject(entry.object))
		}
	}
	for _, entry := range append(entries, rootEntry{step: treeStep{directory: absolute}}) {
		if err := w.runTreeStep(ctx, entry.step); err != nil {
			return err
		}
	}
	return w.removeRealmDirectoryIfEmpty(ctx, parent)
}

// removeRealmDirectoryIfEmpty removes the directory of the dev realms when no
// realm is left in it. Another run may create a realm in the moment between
// the listing and the removal; the server then refuses to remove a directory
// that is not empty, and the directory stays for that run.
func (w *Writer) removeRealmDirectoryIfEmpty(ctx context.Context, parent string) error {
	if w.dryRun {
		return nil
	}
	left, err := w.scheme.ListDirectory(ctx, parent)
	if err != nil {
		return err
	}
	if len(left) > 0 {
		return nil
	}
	if err := w.scheme.RemoveDirectory(ctx, parent); err != nil && !isSchemeError(err) {
		return fmt.Errorf("ydb: remove directory %s: %w", parent, err)
	}
	return nil
}

// RealmIdentity names the dev database the writer's root is, for a lock that
// serializes the runs using it and for the check that refuses a dev database
// that is the target: the database's path, the moment the server created the
// database's root, and the realm's directory when the root is a realm.
//
// A path alone cannot tell two servers apart: every local-ydb container serves
// /local. The creation moment does. Measured on 26.2.1.14 and 25.1.4.7 with two
// containers started a second apart, DescribePath answers /local with
// created_at plan_step 1791098831240 on one and 1791098831800 on the other,
// tx_id 1 on both. Two databases created in the same millisecond on two
// servers share an identity and are treated as one, which refuses a pair that
// was distinct rather than accepting one that was not. A server that reports
// no creation moment is identified by the path alone, as a PostgreSQL database
// is by its name.
func (w *Writer) RealmIdentity(ctx context.Context) (string, error) {
	if w.scheme == nil {
		return "", errors.New("no YDB scheme connection")
	}
	described, err := w.scheme.DescribePath(ctx, w.database)
	if err != nil {
		return "", err
	}
	identity := w.database
	if created := described.GetCreatedAt(); created.GetPlanStep() != 0 || created.GetTxId() != 0 {
		identity = fmt.Sprintf("%s@%d.%d", w.database, created.GetPlanStep(), created.GetTxId())
	}
	if w.root != w.database {
		identity += strings.TrimPrefix(w.root, w.database)
	}
	return identity, nil
}

// TablesNamed lists the row tables under the writer's root whose name is one
// of names, in every directory [Writer.DropAllTables] enters, each with its
// directory relative to the root as its schema. It is how a cleanup plan
// names the migrator's tables, which the reader leaves out of every directory
// and DropAllTables drops with the rest.
func (w *Writer) TablesNamed(ctx context.Context, names []string) ([]dbreset.Object, error) {
	if w.scheme == nil {
		return nil, errors.New("no YDB scheme connection")
	}
	var found []dbreset.Object
	var walk func(dir string) error
	walk = func(dir string) error {
		listed, err := w.scheme.ListDirectory(ctx, path.Join(w.root, dir))
		if err != nil {
			return err
		}
		slices.SortFunc(listed, func(a, b *Ydb_Scheme.Entry) int { return strings.Compare(a.GetName(), b.GetName()) })
		for _, entry := range listed {
			name := entry.GetName()
			switch {
			case entry.GetType() == Ydb_Scheme.Entry_TABLE && slices.Contains(names, name):
				found = append(found, dbreset.Object{Kind: objectKind(entry.GetType()), Schema: dir, Name: name})
			case entry.GetType() == Ydb_Scheme.Entry_DIRECTORY && !strings.HasPrefix(name, ".") &&
				(dir != "" || name != ydburl.RealmDirectory):
				if err := walk(path.Join(dir, name)); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := walk(""); err != nil {
		return nil, err
	}
	return found, nil
}
