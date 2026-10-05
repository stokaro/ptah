package ydbindex

import (
	"ptah.run/core/ast"
	"ptah.run/internal/ydbpartition"
)

// ParseDeclaration reads the partitioning attributes of one index declaration
// out of values, keyed by attribute name, and ignores every other key. It
// returns nil where none is present. The attributes are the ones a table
// declares its splitting and read replicas with, read the same way; see
// [ydbpartition.ParseDeclared], and [ydbpartition.DeclarationError] for the
// error a value YDB would refuse gives.
func ParseDeclaration(values map[string]string) (*ast.IndexPartitioningSpec, error) {
	declared, present, err := ydbpartition.ParseDeclared(values)
	if err != nil {
		return nil, err
	}
	if !present {
		return nil, nil
	}
	return specOf(declared), nil
}
