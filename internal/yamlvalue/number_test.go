package yamlvalue_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/yamlvalue"
)

func TestExactNumber(t *testing.T) {
	cases := []struct{ tag, input, want string }{
		{"!!int", "340282366920938463463374607431768211457", "340282366920938463463374607431768211457"},
		{"!!int", "0x100000000000000000000000000000001", "340282366920938463463374607431768211457"},
		{"!!int", "-0b1010_0101", "-165"},
		{"!!int", "+0o777", "511"},
		{"!!int", "0777", "511"},
		{"!!float", "0777", "511"},
		{"!!float", "+0008.001_2300E+0004", "8.0012300e+0004"},
		{"!!float", "-.000000000000000000000000001", "-0.000000000000000000000000001"},
		{"!!float", "1.234567890123456789e1000", "1.234567890123456789e1000"},
		{"!!float", "1.234567890123456789e-1000", "1.234567890123456789e-1000"},
		{"!!float", "1.e999999999999999999999999999999999", "1e999999999999999999999999999999999"},
		{"!!float", "000.00", "0.00"},
	}
	for _, test := range cases {
		t.Run(test.tag+"/"+test.input, func(t *testing.T) {
			c := qt.New(t)
			got, err := yamlvalue.ExactNumber(test.tag, test.input)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

func TestExactNumberRefusesInvalidOrNonFiniteScalars(t *testing.T) {
	cases := []struct{ tag, input string }{
		{"!!str", "123"}, {"!!int", "1.5"}, {"!!int", "08"}, {"!!int", "0xG"},
		{"!!float", ""}, {"!!float", "--1"}, {"!!float", "1e"}, {"!!float", "1/2"},
		{"!!int", "_123"}, {"!!float", ".1__2"}, {"!!float", "1p3"}, {"!!float", ".inf"}, {"!!float", "-.Inf"}, {"!!float", ".NaN"},
	}
	for _, test := range cases {
		t.Run(test.tag+"/"+test.input, func(t *testing.T) {
			c := qt.New(t)
			got, err := yamlvalue.ExactNumber(test.tag, test.input)
			c.Assert(err, qt.IsNotNil)
			c.Assert(got, qt.Equals, "")
		})
	}
}

func ExampleExactNumber() {
	number, err := yamlvalue.ExactNumber("!!int", "0x20000000000001")
	fmt.Println(number, err)
	// Output: 9007199254740993 <nil>
}
