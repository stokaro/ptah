package triggerdef_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/triggerdef"
)

// TestWhen reads the WHEN condition out of definitions pg_get_triggerdef
// printed on PostgreSQL 18.6.
func TestWhen(t *testing.T) {
	tests := []struct {
		name       string
		definition string
		want       string
	}{
		{
			name:       "a condition over two pseudo-records",
			definition: "CREATE TRIGGER t4 BEFORE UPDATE ON public.x FOR EACH ROW WHEN (((new.total IS DISTINCT FROM old.total) AND (new.a > 0))) EXECUTE FUNCTION f()",
			want:       "((new.total IS DISTINCT FROM old.total) AND (new.a > 0))",
		},
		{
			name:       "a single comparison",
			definition: "CREATE TRIGGER t8 BEFORE INSERT ON public.x FOR EACH ROW WHEN ((new.a = 1)) EXECUTE FUNCTION f()",
			want:       "(new.a = 1)",
		},
		{
			name:       "a parenthesis inside a string literal",
			definition: "CREATE TRIGGER t BEFORE INSERT ON public.x FOR EACH ROW WHEN ((new.label <> ')'::text)) EXECUTE FUNCTION f()",
			want:       "(new.label <> ')'::text)",
		},
		{
			name:       "a statement-level trigger with a condition",
			definition: "CREATE TRIGGER t AFTER INSERT ON public.x FOR EACH STATEMENT WHEN (true) EXECUTE FUNCTION g()",
			want:       "true",
		},
		{
			name:       "no condition",
			definition: "CREATE TRIGGER t1 BEFORE INSERT OR UPDATE ON public.x FOR EACH ROW EXECUTE FUNCTION f()",
			want:       "",
		},
		{
			name:       "transition tables and no condition",
			definition: "CREATE TRIGGER t6 AFTER UPDATE ON public.x REFERENCING OLD TABLE AS oldrows NEW TABLE AS newrows FOR EACH STATEMENT EXECUTE FUNCTION g()",
			want:       "",
		},
		{
			name:       "an unbalanced clause",
			definition: "CREATE TRIGGER t BEFORE INSERT ON public.x FOR EACH ROW WHEN ((new.a = 1) EXECUTE FUNCTION f()",
			want:       "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(triggerdef.When(test.definition), qt.Equals, test.want)
		})
	}
}
