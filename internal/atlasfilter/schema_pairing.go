package atlasfilter

// PairSchemas returns kept, the schemas one side of a comparison keeps after
// a projection, with each schema of all that other names and kept lacks. all
// is that side before the projection, and other is the names the other
// side's projection kept. The result lists the schemas in the order all does,
// and a schema kept already keeps its projected entry. kept is returned as it
// came when nothing is restored, and a new slice otherwise.
//
// A projection keeps a schema where the schema owns a selected object, and
// decides that for each side alone. With --include db.u, a server holding
// db.u keeps the database db, and a desired schema that declares db and not
// db.u loses it: the comparison then drops db with every table the selection
// left in it. The other way round, it creates a database the server holds.
// Paired, a schema one side keeps stays on the other wherever that side has
// it, and a schema neither side keeps stays out of both, so a selector that
// reaches every schema does not bring back the schemas it found nothing in.
func PairSchemas[T any](all, kept []T, other []string, name func(T) string) []T {
	if len(other) == 0 {
		return kept
	}
	wanted := make(map[string]struct{}, len(other))
	for _, schema := range other {
		wanted[schema] = struct{}{}
	}
	projected := make(map[string]T, len(kept))
	for _, schema := range kept {
		projected[name(schema)] = schema
	}
	listed := make(map[string]struct{}, len(all))
	restored := false
	for _, schema := range all {
		listed[name(schema)] = struct{}{}
		_, held := projected[name(schema)]
		_, asked := wanted[name(schema)]
		restored = restored || asked && !held
	}
	if !restored {
		return kept
	}
	out := make([]T, 0, len(kept)+len(other))
	for _, schema := range all {
		if entry, held := projected[name(schema)]; held {
			out = append(out, entry)
			continue
		}
		if _, asked := wanted[name(schema)]; asked {
			out = append(out, schema)
		}
	}
	for _, schema := range kept {
		if _, inAll := listed[name(schema)]; !inAll {
			out = append(out, schema)
		}
	}
	return out
}
