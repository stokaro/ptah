// Package planstyle declares the statement style Ptah's commands plan a schema
// change in. It sits between the command trees and [planext.Options]: the
// native verbs that plan, apply or write a migration, and the compatibility
// verbs that share their planning path, read the style from here rather than
// each choosing one.
package planstyle

import "ptah.run/core/planext"

// Statements is the statement style of every plan a Ptah command shows, writes
// or applies to the database it was asked to change: `schema apply`, `schema
// plan`, `schema diff`, `schema compare`, `migrations plan`, `migrations
// generate` and `migrations checkpoint`, and `schema apply`, `schema plan`,
// `schema diff` and `migrate diff` in ptah-compat. There is no setting that
// selects another.
//
// A plan in this style drops with CASCADE only where it relies on CASCADE to
// remove a dependent it knows of. A dependent nobody declared, such as a view
// in a schema the run leaves out, stops the drop with the server's dependency
// error, where CASCADE would remove it and its data in silence. A guard such
// as IF EXISTS appears only where an earlier statement of the same plan may
// already have removed the object: `schema apply` plans against the database
// it has just read, so any other guard protects nothing there, and ptah-compat
// alone keeps one behind an enum rebuild; see
// [planext.Options.EnumRebuildKeepsGuards]. A migration
// file run against a database in another state than the one it was planned
// against stops at the first statement that does not fit, and on PostgreSQL,
// where a file runs in one transaction unless it opts out, nothing of it stays
// applied. The community binary writes its migrations this way too, and the
// files `migrate diff` writes sit beside the files that binary wrote.
//
// A planner that does not honor the style plans as it would in
// [planext.StatementStyleAnnotated]; see [planext.StatementStyleExact] for
// which do.
const Statements = planext.StatementStyleExact
