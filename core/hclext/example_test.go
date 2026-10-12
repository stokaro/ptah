package hclext_test

import (
	"context"
	"fmt"
	"io"
	"os"

	"ptah.run/core/hclext"
)

// noteReader reads `note` blocks, and declares nothing: the example shows the
// claims an owner receives.
type noteReader struct{ output io.Writer }

func (r noteReader) DecodeHCL(_ context.Context, request hclext.Request) (hclext.Result, error) {
	for _, claim := range request.Claims {
		fmt.Fprintf(r.output, "%s %s.%s: %q\n", claim.Block, claim.Schema, claim.Name, claim.Attributes["text"].Text)
	}
	return hclext.Result{Declarations: make([]hclext.Declaration, len(request.Claims))}, nil
}

// ExampleNewSet shows an owner registering a block as data: the frontend asks
// the set for the shape of a block it meets, reads the block against it, and
// hands the owner every claim of a document in one call.
func ExampleNewSet() {
	set, err := hclext.NewSet(hclext.Extension{
		Owner: "example.org/notes",
		Blocks: []hclext.Block{{Site: hclext.TopLevel, Type: "note", Naming: hclext.SchemaAndName,
			Attributes: []hclext.Attribute{{Name: "text", Kind: hclext.String}}}},
		Decoder: noteReader{output: os.Stdout},
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	shape, found := set.Lookup(hclext.TopLevel, "note")
	fmt.Println(found)

	_, err = set.Decode(context.Background(), []hclext.Match{{Shape: shape, Claim: hclext.Claim{
		Block: "note", Schema: "app", Name: "welcome",
		Attributes: map[string]hclext.Value{"text": {Kind: hclext.String, Text: "hello"}},
	}}})
	if err != nil {
		fmt.Println(err)
	}
	// Output:
	// true
	// note app.welcome: "hello"
}

// ExampleSet_ForDialect selects a custom target without changing the
// unscoped view. Owners publish dialect names as data.
func ExampleSet_ForDialect() {
	set, err := hclext.NewSet(hclext.Extension{
		Owner: "example.org/notes",
		Blocks: []hclext.Block{{Site: hclext.TopLevel, Type: "note", Naming: hclext.SchemaAndName,
			Dialects: []string{"custom"}, Attributes: []hclext.Attribute{{Name: "text", Kind: hclext.String}}}},
		Decoder: noteReader{output: os.Stdout},
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	selected := set.ForDialect("CUSTOM")
	_, scoped := selected.Lookup(hclext.TopLevel, "note")
	_, unscoped := set.Lookup(hclext.TopLevel, "note")
	fmt.Println(selected.Dialect(), scoped, unscoped)
	// Output:
	// custom true false
}
