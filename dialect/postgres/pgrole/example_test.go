package pgrole_test

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/dialect/postgres/pgrole"
)

// ExampleChanges compares a declaration with what the server keeps for one
// role. The declaration sets statement_timeout and resets work_mem by name;
// the role also holds lock_timeout, which no declaration names, so the change
// leaves it alone. A declared value is written in SQL and the observed one is
// the text pg_db_role_setting keeps, so '45s' is compared with 30s in the form
// the server stores. Once the role holds 45s, the declaration plans nothing.
func ExampleChanges() {
	declared := []pgrole.Setting{
		{Name: "statement_timeout", Value: "'45s'"},
		{Name: "work_mem", Reset: true},
	}
	observed := []pgrole.Setting{
		{Name: "lock_timeout", Value: "5s"},
		{Name: "statement_timeout", Value: "30s"},
		{Name: "work_mem", Value: "8MB"},
	}

	change := pgrole.Changes(declared, observed)

	statements, err := (&pgrole.AlterSettings{Role: "app", Change: *change}).Statements()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(strings.Join(statements, "\n"))
	names := make([]string, 0, len(change.Settings))
	for _, setting := range change.Settings {
		names = append(names, setting.Name)
	}
	fmt.Println("changed:", strings.Join(names, ", "))
	unmanaged := slices.ContainsFunc(change.Settings, func(setting pgrole.SettingChange) bool { return setting.Name == "lock_timeout" })
	fmt.Println("lock_timeout in the change:", unmanaged)
	converged := []pgrole.Setting{{Name: "statement_timeout", Value: "45s"}}
	fmt.Println("converged:", pgrole.Changes(declared[:1], converged) == nil)

	// Output:
	// ALTER ROLE "app" SET "statement_timeout" = '45s';
	// ALTER ROLE "app" RESET "work_mem";
	// changed: statement_timeout, work_mem
	// lock_timeout in the change: false
	// converged: true
}
