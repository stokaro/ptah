package query_test

import (
	"database/sql"
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/query"
)

// ExampleUpsertInto renders YDB's UPSERT INTO, which writes each row over the
// row with the same primary key and inserts it where there is none. Every
// argument is a sql.NamedArg named after its placeholder, because a YDB
// connection binds a parameter by name; an int64 binds as Int64, so a column of
// another integer type takes a value of its own Go type.
func ExampleUpsertInto() {
	stmt := query.UpsertInto("users").
		Columns("id", "name").
		Values(int64(1), "alice").
		Values(int64(2), "bob").
		Build()

	text, args, err := query.RenderInsert(stmt, platform.YDB)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(text)
	for _, arg := range args {
		named := arg.(sql.NamedArg)
		fmt.Printf("%s=%v\n", named.Name, named.Value)
	}

	// Output:
	// UPSERT INTO `users` (`id`, `name`) VALUES ($p1, $p2), ($p3, $p4)
	// p1=1
	// p2=alice
	// p3=2
	// p4=bob
}

// ExampleRenderSelectWithCapabilities renders one paged query for two YDB
// release lines and shows what the line decides: both take LIMIT and OFFSET as
// Uint64, and the 25.1 line refuses an UPDATE with RETURNING that 26.2 runs.
func ExampleRenderSelectWithCapabilities() {
	page := query.Select("id").From("users").OrderBy(query.Asc("id")).Limit(10).Offset(20).Build()
	text, args, err := query.RenderSelectWithCapabilities(page, platform.YDB, capability.YDB251())
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(text, len(args))

	update := query.Update("users").Set("name", "bob").Where(query.Eq("id", int64(7))).Returning("id").Build()
	for _, caps := range []capability.Capabilities{capability.YDB262(), capability.YDB251()} {
		text, _, err := query.RenderUpdateWithCapabilities(update, platform.YDB, caps)
		fmt.Println(text, err)
	}

	// Output:
	// SELECT `id` FROM `users` ORDER BY `id` ASC LIMIT $p1 OFFSET $p2 2
	// UPDATE `users` SET `name` = $p1 WHERE `id` = $p2 RETURNING `id` <nil>
	//  renderer: RETURNING, which requires target capability returning_clause, unavailable on this ydb target
}
