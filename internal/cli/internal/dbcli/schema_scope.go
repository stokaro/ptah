package dbcli

import "ptah.run/internal/schemascope"

// ParseSchemasForURLs is [ParseSchemas] with the native selection preflight
// for each live source URL. A MySQL-family URL naming a database cannot read
// another database in the list. Checking every side before opening any of them
// keeps schema diff from connecting to --from before refusing --to. Local
// source paths and non-MySQL URLs retain their own scope rules.
func ParseSchemasForURLs(raw string, addresses ...string) ([]string, error) {
	schemas, err := ParseSchemas(raw)
	if err != nil {
		return nil, err
	}
	for _, address := range addresses {
		if err := schemascope.RefuseOutsideURLDatabase(address, schemas); err != nil {
			return nil, err
		}
	}
	return schemas, nil
}
