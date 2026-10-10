// Package mysqlsource reads MySQL and MariaDB table, index and column
// settings from the platform properties of a source and writes them back, and
// reads an index's block-size hint from the Go index directive. Frontends read
// the syntax; the settings' meaning stays with the owner's models.
package mysqlsource
