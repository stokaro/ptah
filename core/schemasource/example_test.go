package schemasource_test

import (
	"context"
	"fmt"
	"os"
	"regexp"

	"go.5x5.cz/ptah/core/schemasource"
)

// ExampleRun executes an external program that prints a desired schema to its
// standard output and parses that output into the same *schemamodel.Database
// that Go annotations, YAML, HCL, and SQL files produce, so nothing downstream
// can tell how the desired state was authored. The program here is this test
// binary re-executing itself as a fixture; in real use Args names an ORM's
// schema exporter or any other loader that can print the schema.
func ExampleRun() {
	// The argv is executed directly, never through a shell, so each argument
	// is one element and no quoting or expansion is applied. The first two
	// environment entries select the fixture's behavior; GORACE keeps a
	// race-instrumented run from sleeping at exit.
	db, err := schemasource.Run(context.Background(), schemasource.Command{
		Args: []string{os.Args[0], "-test.run=TestHelperProcess"},
		Env: []string{
			"GO_WANT_HELPER_PROCESS=1",
			"SCHEMASOURCE_HELPER_MODE=yaml",
			"GORACE=atexit_sleep_ms=0",
		},
		Format: "yaml",
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	for _, table := range db.Tables {
		fmt.Println("table", table.Name)
	}
	for _, field := range db.Fields {
		fmt.Printf("column %s.%s %s\n", field.StructName, field.Name, field.Type)
	}

	// Output:
	// table widgets
	// column widgets.id INTEGER
	// column widgets.name TEXT
}

// ExampleRun_emptyOutput shows the package's signature guarantee: a provider
// that prints nothing is an error, never an empty desired schema — downstream
// comparison would read an empty desired state as intent to remove what the
// database holds. The fixture is the test binary re-executing itself and
// exiting successfully without output.
func ExampleRun_emptyOutput() {
	_, err := schemasource.Run(context.Background(), schemasource.Command{
		Args: []string{os.Args[0], "-test.run=TestHelperProcess"},
		Env: []string{
			"GO_WANT_HELPER_PROCESS=1",
			"SCHEMASOURCE_HELPER_MODE=empty",
			"GORACE=atexit_sleep_ms=0",
		},
	})

	// The message quotes the program, which here is the test binary's
	// temporary path; substitute a stable name so the output stays printable.
	msg := regexp.MustCompile(`^schema command "[^"]*"`).
		ReplaceAllString(err.Error(), `schema command "schema-exporter"`)
	fmt.Println(msg)

	// Output:
	// schema command "schema-exporter" produced empty output
}

// ExampleRun_environment shows the environment confinement Run applies before
// any process starts: PATH cannot be overridden through Command.Env, because a
// changed PATH would silently change which program a bare Args[0] resolves to.
// The same pre-spawn validation refuses a PWD override — Command.Dir is what
// chooses the working directory — and any entry that is not KEY=VALUE.
func ExampleRun_environment() {
	_, err := schemasource.Run(context.Background(), schemasource.Command{
		Args: []string{"schema-exporter"},
		Env:  []string{"PATH=/opt/tools/bin"},
	})
	fmt.Println(err)

	// Output:
	// schema command environment must not override PATH; use an explicit executable path
}
