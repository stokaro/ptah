// Package migrationfile is the migration file-layout toolkit: the names,
// directory formats, directives, and embedded sections a migration file is
// made of, with no database connection and no execution.
//
// The package answers questions a tool can ask about migration files without
// running them: which files a directory contributes and in which format
// ([Discover], [DirFormat]), what a file name means ([ParseFileName],
// [ParseAtlasFileName], [File]), what its directive header declares
// ([ParseDirectives], [ParseTimeouts], [ParseFileTxMode]), and how an Atlas
// txtar archive or SQL template unfolds into executable content
// ([ParseAtlasTxtar], [RenderAtlasTemplateSQL], [ParseUp]).
//
// The migrator engine builds on this package to load and apply migrations;
// linters, importers, and compatibility tooling use it directly. The
// dependency is one-directional: nothing here reaches back into execution,
// revision state, or a live connection.
package migrationfile
