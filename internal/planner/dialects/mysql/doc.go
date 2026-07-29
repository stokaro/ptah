// Package mysql implements the MySQL migration planner dialect, transforming
// schema differences into MySQL DDL operations. Modifier intent recorded on
// the planned AST (such as IF EXISTS) is later validated by the renderer
// capability layer, which drops what the concrete MySQL target does not
// accept.
package mysql
