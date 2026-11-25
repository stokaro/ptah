package descriptor_test

import (
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/descriptor"
)

// withRounding is a descriptor whose rounding conditions are rounding.
func withRounding(rounding descriptor.Rounding) descriptor.Dialect {
	described := head("alpha")
	described.Rounding = rounding
	return described
}

func TestNewRegistry_Rounding_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		rounding descriptor.Rounding
	}{
		{name: "no condition", rounding: descriptor.Rounding{}},
		{name: "a decimal condition alone", rounding: descriptor.Rounding{Decimal: "{column} <> CAST({column} AS {type})"}},
		{name: "both conditions, the cast spellings and the domains", rounding: descriptor.Rounding{
			Decimal:   "{column} <> CAST({column} AS {type})",
			Temporal:  "CAST({column} AS {domain}) <> CAST(CAST({column} AS {type}) AS {domain})",
			CastTypes: map[string]string{"int": "SIGNED", "timestamp": "DATETIME{args}"},
			Domains:   map[string]string{"datetime": "datetime2(7)"},
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			described := withRounding(test.rounding)

			registry, err := descriptor.NewRegistry(described)

			c.Assert(err, qt.IsNil)
			got, found := registry.Lookup("alpha")
			c.Assert(found, qt.IsTrue)
			c.Assert(got.Equal(described), qt.IsTrue)
		})
	}
}

func TestNewRegistry_Rounding_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		rounding descriptor.Rounding
		want     string
	}{
		{
			name:     "a decimal condition with no column",
			rounding: descriptor.Rounding{Decimal: "c <> CAST(c AS {type})"},
			want:     `invalid dialect descriptor: dialect "alpha": the rounding condition "c <> CAST(c AS {type})" for a decimal has no {column}`,
		},
		{
			name:     "a decimal condition with no type",
			rounding: descriptor.Rounding{Decimal: "{column} <> ROUND({column})"},
			want:     `invalid dialect descriptor: dialect "alpha": the rounding condition "{column} <> ROUND({column})" for a decimal has no {type}`,
		},
		{
			name:     "a temporal condition with no type",
			rounding: descriptor.Rounding{Temporal: "MICROSECOND({column}) <> 0"},
			want:     `invalid dialect descriptor: dialect "alpha": the rounding condition "MICROSECOND({column}) <> 0" for a temporal type has no {type}`,
		},
		{
			name:     "a cast name in capitals",
			rounding: descriptor.Rounding{Decimal: "{column} <> CAST({column} AS {type})", CastTypes: map[string]string{"INT": "SIGNED"}},
			want:     `invalid dialect descriptor: dialect "alpha": the rounding cast type "INT" -> "SIGNED" needs a lowercase name and a spelling`,
		},
		{
			name:     "a cast name with no spelling",
			rounding: descriptor.Rounding{Decimal: "{column} <> CAST({column} AS {type})", CastTypes: map[string]string{"int": " "}},
			want:     `invalid dialect descriptor: dialect "alpha": the rounding cast type "int" -> " " needs a lowercase name and a spelling`,
		},
		{
			name:     "a domain name in capitals",
			rounding: descriptor.Rounding{Temporal: "{column} <> CAST({column} AS {type})", Domains: map[string]string{"Interval": "interval"}},
			want:     `invalid dialect descriptor: dialect "alpha": the rounding domain "Interval" -> "interval" needs a lowercase name and a spelling`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			registry, err := descriptor.NewRegistry(withRounding(test.rounding))

			c.Assert(err, qt.ErrorIs, descriptor.ErrInvalidDescriptor)
			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(test.want))
			c.Assert(registry, qt.IsNil)
		})
	}
}
