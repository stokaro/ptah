package ptaherr_test

import (
	"errors"
	"fmt"

	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
)

// Example shows the package's two-layer branching contract on one returned
// error: errors.Is against a sentinel selects the failure class, and
// errors.AsType against the structured type reads the context the failure
// carried. The error is a real one -- renderer.NewRenderer refuses a dialect
// Ptah cannot render -- not one constructed by hand. Branch on the sentinel
// and read the fields: the message text is a diagnostic for a person, not a
// value to match on.
func Example() {
	_, err := renderer.NewRenderer("dbase")

	if errors.Is(err, ptaherr.ErrUnsupportedDialect) {
		fmt.Println("branch: unsupported dialect")
	}

	if renderErr, ok := errors.AsType[*ptaherr.RenderError](err); ok {
		fmt.Println("dialect:", renderErr.Dialect)
	}

	// Output:
	// branch: unsupported dialect
	// dialect: dbase
}

// ExampleParseError feeds the annotation parser source carrying an attribute
// no directive declares, then reads the refusal both ways: errors.AsType
// reaches the location fields that say where and what, and errors.Is through
// Unwrap reaches the sentinel that says which kind of refusal it was -- an
// unknown attribute is a typo, a retired one was spelled correctly and
// refused for a stated reason, and the sentinels keep the two branches apart.
func ExampleParseError() {
	source := `package models

//ptah:schema:table name="products"
type Product struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true" bogus="yes"
	ID int64
}
`

	_, err := goschema.ParseSource("models.go", source)

	if parseErr, ok := errors.AsType[*ptaherr.ParseError](err); ok {
		fmt.Printf("%s:%d on %s: attribute %q\n",
			parseErr.File, parseErr.Line, parseErr.Directive, parseErr.Attribute)
	}
	fmt.Println("unknown attribute:", errors.Is(err, ptaherr.ErrUnknownAttribute))
	fmt.Println("retired attribute:", errors.Is(err, ptaherr.ErrRetiredAttribute))

	// Output:
	// models.go:5 on ptah:schema:field: attribute "bogus"
	// unknown attribute: true
	// retired attribute: false
}
