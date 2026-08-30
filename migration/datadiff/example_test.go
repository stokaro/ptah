package datadiff_test

import (
	"fmt"

	"go.5x5.cz/ptah/migration/datadiff"
)

// ExampleCompute diffs the desired managed rows of a table against what the
// database currently holds. Rows are matched by the key columns; each
// differing row comes back as an insert (desired only), an update (both sides
// captured), or a delete (live only) — a matched row whose managed columns are
// equal is not reported — and every slice is sorted by key so the result is
// deterministic. This is the entry point for the pure computation — no
// database connection is involved.
func ExampleCompute() {
	desired := []datadiff.Row{
		{"id": 1, "name": "Czechia"},
		{"id": 2, "name": "Austria"},
	}
	live := []datadiff.Row{
		{"id": 1, "name": "Czech Republic"},
		{"id": 3, "name": "Zeta"},
	}

	diff, err := datadiff.Compute("", "regions", []string{"id"}, desired, live)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("inserts:", len(diff.Inserts))
	fmt.Println("updates:", len(diff.Updates))
	fmt.Println("deletes:", len(diff.Deletes))
	u := diff.Updates[0]
	fmt.Printf("update id=%v: %q -> %q\n", u.Key["id"], u.Live["name"], u.Desired["name"])

	// Output:
	// inserts: 1
	// updates: 1
	// deletes: 1
	// update id=1: "Czech Republic" -> "Czechia"
}

// ExampleCompute_nullVersusEmptyString shows the normalized value comparison at
// its two edges. The one-byte type tag keeps a live SQL NULL distinct from a
// desired empty string, so row 1 is reported as an update rather than silently
// matching; the driver-agnostic "V" form lets a desired int compare equal to
// the int64 a driver scans back, so row 2 reports nothing.
func ExampleCompute_nullVersusEmptyString() {
	desired := []datadiff.Row{
		{"id": 1, "note": ""},
		{"id": 2, "rank": 5},
	}
	live := []datadiff.Row{
		{"id": 1, "note": nil},
		{"id": 2, "rank": int64(5)},
	}

	diff, err := datadiff.Compute("", "entries", []string{"id"}, desired, live)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("updates:", len(diff.Updates))
	fmt.Printf("update id=%v: live NULL differs from desired %q\n",
		diff.Updates[0].Key["id"], diff.Updates[0].Desired["note"])

	// Output:
	// updates: 1
	// update id=1: live NULL differs from desired ""
}

// ExampleRender turns a DataDiff into a pair of SQL scripts: up applies the
// desired state in Inserts, Updates, Deletes order, and down is the exact
// inverse in fully reversed order. Columns are sorted inside every statement,
// identifiers are quoted for the dialect, and a set Schema qualifies the table
// name.
func ExampleRender() {
	diff := &datadiff.DataDiff{
		Schema: "app",
		Table:  "regions",
		Keys:   []string{"code"},
		Inserts: []datadiff.Row{
			{"code": "AT", "name": "Austria"},
		},
		Updates: []datadiff.RowUpdate{{
			Key:     map[string]any{"code": "CZ"},
			Desired: datadiff.Row{"code": "CZ", "name": "Czechia"},
			Live:    datadiff.Row{"code": "CZ", "name": "Czech Republic"},
		}},
		Deletes: []datadiff.Row{
			{"code": "ZZ", "name": "Zeta"},
		},
	}

	up, down, err := datadiff.Render(diff, "postgres")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Print("-- up\n", up, "-- down\n", down)

	// Output:
	// -- up
	// INSERT INTO "app"."regions" ("code", "name") VALUES ('AT', 'Austria');
	// UPDATE "app"."regions" SET "name" = 'Czechia' WHERE "code" = 'CZ';
	// DELETE FROM "app"."regions" WHERE "code" = 'ZZ';
	// -- down
	// INSERT INTO "app"."regions" ("code", "name") VALUES ('ZZ', 'Zeta');
	// UPDATE "app"."regions" SET "name" = 'Czech Republic' WHERE "code" = 'CZ';
	// DELETE FROM "app"."regions" WHERE "code" = 'AT';
}

// ExampleRender_roundTrip runs the whole pipeline — Compute, then Render — for
// a single changed row, to show why RowUpdate carries both sides: the forward
// UPDATE sets the Desired value and the down UPDATE restores the Live value it
// replaced, so applying up followed by down returns the table to its original
// contents.
func ExampleRender_roundTrip() {
	desired := []datadiff.Row{{"code": "CZ", "name": "Czechia"}}
	live := []datadiff.Row{{"code": "CZ", "name": "Czech Republic"}}

	diff, err := datadiff.Compute("", "regions", []string{"code"}, desired, live)
	if err != nil {
		fmt.Println(err)
		return
	}
	up, down, err := datadiff.Render(diff, "postgres")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Print(up, down)

	// Output:
	// UPDATE "regions" SET "name" = 'Czechia' WHERE "code" = 'CZ';
	// UPDATE "regions" SET "name" = 'Czech Republic' WHERE "code" = 'CZ';
}
