package triggerdef_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/triggerdef"
)

// TestWhen reads the WHEN condition out of the definitions pg_get_triggerdef
// printed on PostgreSQL 18.6, CockroachDB 26.3.1 and YugabyteDB 2024.2.10, and
// out of the edges of that grammar: a pair that does not enclose the whole
// condition, and the clause's closing keyword inside a string.
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
			name:       "CockroachDB 26.3 writes the condition without the outer pair",
			definition: "CREATE TRIGGER t_w1 BEFORE UPDATE ON public.x FOR EACH ROW WHEN (new).a > 0:::INT8 EXECUTE FUNCTION public.f()",
			want:       "(new).a > 0:::INT8",
		},
		{
			name:       "CockroachDB 26.3 with a constant condition",
			definition: "CREATE TRIGGER t_w2 BEFORE UPDATE ON public.x FOR EACH ROW WHEN true EXECUTE FUNCTION public.f()",
			want:       "true",
		},
		{
			name:       "a pair that does not enclose the whole condition",
			definition: "CREATE TRIGGER t BEFORE UPDATE ON public.x FOR EACH ROW WHEN (new.a > 0) AND (new.b > 0) EXECUTE FUNCTION f()",
			want:       "(new.a > 0) AND (new.b > 0)",
		},
		{
			name:       "EXECUTE inside a string literal",
			definition: "CREATE TRIGGER t BEFORE UPDATE ON public.x FOR EACH ROW WHEN ((new.s <> ' EXECUTE '::text)) EXECUTE FUNCTION f()",
			want:       "(new.s <> ' EXECUTE '::text)",
		},
		{
			name:       "YugabyteDB 2024.2 writes EXECUTE PROCEDURE",
			definition: "CREATE TRIGGER t_when BEFORE UPDATE ON public.x FOR EACH ROW WHEN ((new.a = ANY (ARRAY[1, 2]))) EXECUTE PROCEDURE f()",
			want:       "(new.a = ANY (ARRAY[1, 2]))",
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
