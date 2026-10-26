package schemafile

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlext"
	"ptah.run/internal/schemamerge"
	"ptah.run/internal/sqlschema"
)

// readsAsLoad reports whether an owner's grammar reads the SQL documents of
// opts.Dialect, as the YDB owner reads YQL. Such documents are read as one
// load, in one call to the owner, rather than one file at a time; see
// [sqlschema.ReadLoad].
func readsAsLoad(opts Options) bool {
	return sqlschema.ReadsAsLoad(opts.Dialect, opts.SQL)
}

// sqlScript is the SQL documents of one load, in the order the load runs them,
// for a dialect whose documents an owner's grammar reads.
type sqlScript struct {
	files []sqlext.SourceFile
	// dirs are the schema directories among the files. Each is checked on
	// its own, as [loadSchemaDir] checks one, that a later file does not
	// declare again what an earlier one did.
	dirs []scriptDir
}

// scriptDir is one schema directory of a [sqlScript].
type scriptDir struct {
	// first is the index of the directory's first file.
	first int
	// names are the directory's files, as its check names them.
	names []string
}

// addTree adds a SQL schema file and everything it imports, in the order the
// chain runs them; see [walkSQLImports].
func (s *sqlScript) addTree(resolved string) error {
	return walkSQLImports(filepath.Dir(resolved), resolved, make(map[string]struct{}), 0, func(path string, data []byte) error {
		s.files = append(s.files, sqlext.SourceFile{Path: path, Text: string(data)})
		return nil
	})
}

// addDir adds the files of a SQL schema directory, names, as [schemaDirFiles]
// lists them. A directory entry imports nothing, as in [loadSchemaDir].
func (s *sqlScript) addDir(dir string, names []string) error {
	group := scriptDir{first: len(s.files), names: names}
	for _, name := range names {
		resolved, isDir, err := statSchemaPath(filepath.Join(dir, name))
		if err != nil {
			return err
		}
		if isDir {
			return isDirectoryError(resolved)
		}
		data, err := os.ReadFile(resolved)
		if err != nil {
			return fmt.Errorf("read SQL schema file: %w", err)
		}
		s.files = append(s.files, sqlext.SourceFile{Path: resolved, Text: string(data)})
	}
	s.dirs = append(s.dirs, group)
	return nil
}

// read reads the script in one call to the owner, and checks each schema
// directory among its files.
func (s *sqlScript) read(ctx context.Context, opts Options) (*schemamodel.Database, error) {
	root, err := sourceDatabasePath(ctx, opts)
	if err != nil {
		return nil, err
	}
	// The caller context bounds owner parsing across the whole script.
	load, err := sqlschema.ReadLoad(ctx,
		sqlext.GrammarRequest{Target: opts.Dialect, Files: s.files, DatabasePath: root},
		sqlschema.Options{Owners: opts.SQL, Vocabulary: opts.CoverageVocabulary, Relations: opts.Relations})
	if refused, ok := errors.AsType[*sqlext.FileError](err); ok {
		return nil, fmt.Errorf("read SQL schema file %s: %w", refused.Path, refused.Err)
	}
	if err != nil {
		return nil, err
	}
	for _, dir := range s.dirs {
		ledger := newDirDeclarations()
		for i, name := range dir.names {
			if err := ledger.admit(name, load.Files[dir.first+i]); err != nil {
				return nil, err
			}
		}
	}
	database := load.Database
	schemamodel.Finalize(&database)
	return &database, nil
}

// loadSourcesAsLoad is [LoadSources] for a dialect whose documents an owner's
// grammar reads. Every SQL source, a file with its imports or a schema
// directory, joins one load the owner reads in one call, and the sources in
// other formats are read on their own. The load's result takes the place of
// the first SQL source in the merge.
//
// A document of the load can change only what the load declares. A source in
// another format is not part of it, so a statement that changes an object
// such a source declares is refused, and the refusal says so.
func loadSourcesAsLoad(ctx context.Context, sources []Source, opts Options) (*schemamodel.Database, error) {
	var script sqlScript
	parts := make([]*schemamodel.Database, 0, len(sources))
	var others []string
	scriptAt := -1
	for _, source := range sources {
		resolved, isSQL, err := script.addSource(source)
		if err != nil {
			return nil, err
		}
		if isSQL {
			if scriptAt < 0 {
				scriptAt = len(parts)
				parts = append(parts, nil)
			}
			continue
		}
		db, err := LoadPath(ctx, resolved, source.apply(opts))
		if err != nil {
			return nil, err
		}
		parts = append(parts, db)
		others = append(others, filepath.Base(resolved))
	}
	if scriptAt >= 0 {
		db, err := script.read(ctx, opts)
		if errors.Is(err, sqlext.ErrUndeclared) && len(others) > 0 {
			return nil, fmt.Errorf(
				"%w; a %s SQL file changes only what the SQL files of the same load declare, not what %s declares",
				err, opts.Dialect, strings.Join(others, ", "))
		}
		if err != nil {
			return nil, err
		}
		parts[scriptAt] = db
	}
	merged := &schemamodel.Database{}
	for _, part := range parts {
		if err := schemamerge.Append(merged, part); err != nil {
			return nil, err
		}
	}
	schemamodel.Finalize(merged)
	return merged, nil
}

// addSource adds source to the script when it is SQL, a file or a schema
// directory of SQL files, and returns its resolved path either way.
func (s *sqlScript) addSource(source Source) (string, bool, error) {
	path, err := LocalFilePath(source.URL)
	if err != nil {
		return "", false, err
	}
	resolved, isDir, err := statSchemaPath(path)
	if err != nil {
		return "", false, err
	}
	if !isDir {
		if !strings.EqualFold(filepath.Ext(resolved), dirSQLExtension) {
			return resolved, false, nil
		}
		return resolved, true, s.addTree(resolved)
	}
	names, format, err := schemaDirFiles(resolved)
	if err != nil {
		return "", false, err
	}
	if format != schemaDirFormatSQL {
		return resolved, false, nil
	}
	return resolved, true, s.addDir(resolved, names)
}
