package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/atlashcl"
)

func TestParse_PtahParityExtensions_HappyPath(t *testing.T) {
	c := qt.New(t)
	source := []byte(`
schema "app" {}

table "users" {
  schema = schema.app
  checks = ["id > 0"]
  custom = "WITHOUT OIDS"

  platform "mysql" {
    override "engine" {
      value = "InnoDB"
    }
  }

  column "id" {
    type       = "DOUBLE PRECISION"
    check      = "id > 0"
    check_name = "users_id_positive"

    platform "mysql" {
      override "type" {
        value = "BIGINT AUTO_INCREMENT"
      }
    }
  }

  column "status" {
    type = enum_user_status
    enum = ["active", "disabled"]
  }

  column "owner_id" {
    type = bigint
  }

  constraint "users_owner_fk" {
    type            = "FOREIGN KEY"
    columns         = ["owner_id"]
    foreign_table   = "app.accounts"
    foreign_columns = ["id"]
    on_delete       = "CASCADE"
    on_update       = "RESTRICT"
    comment         = "Owner"
  }

  constraint "users_no_overlap" {
    type      = "EXCLUDE"
    using     = "gist"
    elements  = "id WITH ="
    condition = "id > 0"
    comment   = "No overlap"
  }

  index "users_id_idx" {
    columns = [column.id]
    ops     = "gin_trgm_ops"
  }
}

role "app_user" {
  login    = true
  password = "SCRAM-SHA-256$fixture"
}

function "lookup_user" {
  schema = schema.app
  params = "IN user_id BIGINT, OUT display_value DOUBLE PRECISION"
  return = "DOUBLE PRECISION"
  lang   = SQL
  as     = "SELECT user_id::double precision"
}

data {
  table = table.app.users
  keys  = ["id", "owner_id"]
  file  = "../data/users.yaml"
}
`)

	db, err := atlashcl.Parse(source, "configs/schema.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].Checks, qt.DeepEquals, []string{"id > 0"})
	c.Assert(db.Tables[0].CustomSQL, qt.Equals, "WITHOUT OIDS")
	c.Assert(db.Tables[0].Overrides, qt.DeepEquals, map[string]map[string]string{
		"mysql": {"engine": "InnoDB"},
	})
	c.Assert(db.Fields, qt.HasLen, 3)
	c.Assert(db.Fields[0].Type, qt.Equals, "DOUBLE PRECISION")
	c.Assert(db.Fields[0].Check, qt.Equals, "id > 0")
	c.Assert(db.Fields[0].CheckName, qt.Equals, "users_id_positive")
	c.Assert(db.Fields[0].Overrides, qt.DeepEquals, map[string]map[string]string{
		"mysql": {"type": "BIGINT AUTO_INCREMENT"},
	})
	c.Assert(db.Fields[1].Enum, qt.DeepEquals, []string{"active", "disabled"})
	c.Assert(db.Constraints, qt.HasLen, 2)
	c.Assert(db.Constraints[0].ForeignColumns, qt.DeepEquals, []string{"id"})
	c.Assert(db.Constraints[0].OnDelete, qt.Equals, "CASCADE")
	c.Assert(db.Constraints[0].Comment, qt.Equals, "Owner")
	c.Assert(db.Constraints[1].UsingMethod, qt.Equals, "gist")
	c.Assert(db.Constraints[1].ExcludeElements, qt.Equals, "id WITH =")
	c.Assert(db.Constraints[1].WhereCondition, qt.Equals, "id > 0")
	c.Assert(db.Indexes, qt.HasLen, 1)
	c.Assert(db.Indexes[0].Operator, qt.Equals, "gin_trgm_ops")
	c.Assert(db.Roles, qt.HasLen, 1)
	c.Assert(db.Roles[0].Password, qt.Equals, "SCRAM-SHA-256$fixture")
	c.Assert(db.Functions, qt.HasLen, 1)
	c.Assert(db.Functions[0].Parameters, qt.Equals, "in user_id bigint, out display_value double precision")
	c.Assert(db.Functions[0].Returns, qt.Equals, "double precision")
	c.Assert(db.ManagedData, qt.HasLen, 1)
	c.Assert(db.ManagedData[0].Schema, qt.Equals, "app")
	c.Assert(db.ManagedData[0].Table, qt.Equals, "users")
	c.Assert(db.ManagedData[0].Keys, qt.DeepEquals, []string{"id", "owner_id"})
	c.Assert(db.ManagedData[0].File, qt.Equals, "../data/users.yaml")
	c.Assert(db.ManagedData[0].SourceDir, qt.Equals, "configs")
}

func TestParse_PtahParityExtensions_FailurePath(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name  string
		hcl   string
		match string
	}{
		{
			name: "duplicate platform override",
			hcl: `
table "users" {
  platform "mysql" {
    override "engine" { value = "InnoDB" }
    override "engine" { value = "MyISAM" }
  }
}
`,
			match: `.*table platform override "engine" for dialect "mysql" is duplicated.*`,
		},
		{
			name: "unsupported platform attribute",
			hcl: `
table "users" {
  platform "mysql" {
    engine = "InnoDB"
  }
}
`,
			match: `.*unsupported table platform attribute "engine".*`,
		},
		{
			name: "platform override without value",
			hcl: `
table "users" {
  platform "mysql" {
    override "engine" {}
  }
}
`,
			match: `.*table platform override "engine" requires value.*`,
		},
		{
			name: "raw function params mixed with arg blocks",
			hcl: `
function "lookup_user" {
  params = "user_id bigint"
  arg "user_id" { type = bigint }
  return = bigint
  as = "SELECT user_id"
}
`,
			match: `.*function cannot mix params attribute with arg blocks.*`,
		},
		{
			name: "managed data label",
			hcl: `
data "users" {
  table = table.users
  keys  = ["id"]
  file  = "users.yaml"
}
`,
			match: `.*data block does not accept labels.*`,
		},
		{
			name: "managed data keys type",
			hcl: `
data {
  table = table.users
  keys  = [1]
  file  = "users.yaml"
}
`,
			match: `.*keys must be a list of strings.*`,
		},
		{
			name: "managed data keys object",
			hcl: `
data {
  table = table.users
  keys  = { intended_key = "id" }
  file  = "users.yaml"
}
`,
			match: `.*keys must be a list of strings.*`,
		},
		{
			name: "column enum values type",
			hcl: `
table "users" {
  column "status" {
    type = text
    enum = ["active", 1]
  }
}
`,
			match: `.*enum must be a list of strings.*`,
		},
		{
			name: "table checks object",
			hcl: `
table "users" {
  checks = { intended_check = "id > 0" }
}
`,
			match: `.*checks must be a list of strings.*`,
		},
		{
			name: "constraint missing type",
			hcl: `
table "users" {
  constraint "users_id_positive" {
    check = "id > 0"
  }
}
`,
			match: `.*constraint "users_id_positive" requires type.*`,
		},
		{
			name: "unsupported constraint attribute",
			hcl: `
table "users" {
  constraint "users_id_positive" {
    type = "CHECK"
    expr = "id > 0"
  }
}
`,
			match: `.*unsupported constraint attribute "expr".*`,
		},
		{
			name: "unsupported attributes are deterministic",
			hcl: `
table "users" {
  zeta  = "last"
  alpha = "first"
}
`,
			match: `.*unsupported table attribute "alpha".*`,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			_, err := atlashcl.Parse([]byte(test.hcl), "schema.hcl")
			c.Assert(err, qt.ErrorMatches, test.match)
		})
	}
}
