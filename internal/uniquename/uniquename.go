// Package uniquename is the one rule Ptah applies when two objects of one
// generated set derive the same name: the first to claim a name keeps it, and
// each later claimant takes the first free numbered form.
//
// A derived name is not an identity. The SQL schema reader and the live
// database converter both derive a table's struct name by camel-casing its
// table name, so "Docs" and docs derive one struct name, and the model joins a
// table's columns, indexes and constraints through it: two tables sharing one
// are one table to every consumer (stokaro/ptah#3642, stokaro/ptah#3647). The
// Go code generator derives a file name by lower-casing the qualified table
// name, where two tables sharing one overwrite each other on disk. Each of
// them claims its names through [Next], so the numbered forms read the same
// wherever a collision is resolved.
package uniquename

import "strconv"

// Next returns base when taken reports it free, and otherwise the first of
// base2, base3, and so on that taken reports free. The caller records the
// result as taken before claiming the next name; the order of the claims
// decides which object keeps base, so a caller claims in an order its input
// fixes.
func Next(base string, taken func(string) bool) string {
	name := base
	for suffix := 2; taken(name); suffix++ {
		name = base + strconv.Itoa(suffix)
	}
	return name
}
