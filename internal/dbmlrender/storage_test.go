package dbmlrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/dbmlrender"
)

func TestRender_EmptyOptionalStorageSettingsDoNotWarn(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{
		Tables:  []schemamodel.Table{{StructName: "T", Name: "t"}},
		Indexes: []schemamodel.Index{{StructName: "T", Name: "idx"}},
	}

	result, err := renderDBML(c, db, dbmlrender.Options{})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Omitted, qt.HasLen, 0)
}
