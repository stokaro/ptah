package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
)

func TestParseSource_FailurePath_RejectsRemovedBarewordAttributes(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name   string
		source string
		match  string
	}{
		{
			name: "field nullable",
			source: `package models
type User struct {
	//ptah:schema:field name="id" type="BIGINT" nullable
	ID int64
}
`,
			match: `.*unknown annotation attribute "nullable".*`,
		},
		{
			name: "field autoincrement",
			source: `package models
type User struct {
	//ptah:schema:field name="id" type="BIGINT" autoincrement
	ID int64
}
`,
			match: `.*unknown annotation attribute "autoincrement".*`,
		},
		{
			name: "field index",
			source: `package models
type User struct {
	//ptah:schema:field name="id" type="BIGINT" index
	ID int64
}
`,
			match: `.*unknown annotation attribute "index".*`,
		},
		{
			name: "embedded not null",
			source: `package models
type User struct {
	//ptah:embedded mode="json" name="metadata" not_null
	Metadata map[string]any
}
`,
			match: `.*unknown annotation attribute "not_null".*`,
		},
		{
			name: "embedded index",
			source: `package models
type User struct {
	//ptah:embedded mode="relation" field="account_id" ref="accounts(id)" index
	Account Account
}
type Account struct{}
`,
			match: `.*unknown annotation attribute "index".*`,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			_, err := goschema.ParseSource("models.go", test.source)
			c.Assert(err, qt.ErrorMatches, test.match)
		})
	}
}
