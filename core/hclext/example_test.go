package hclext_test

import (
	"context"
	"fmt"

	"ptah.run/core/hclext"
)

// noteReader records claims without declaring objects. Each example prints
// the evaluated data its owner receives.
type noteReader struct{ claims *[]hclext.Claim }

func (r noteReader) DecodeHCL(_ context.Context, request hclext.Request) (hclext.Result, error) {
	*r.claims = append(*r.claims, request.Claims...)
	return hclext.Result{Declarations: make([]hclext.Declaration, len(request.Claims))}, nil
}

// ExampleNewSet shows an owner registering a block as data: the frontend asks
// the set for the shape of a block it meets, reads the block against it, and
// hands the owner every claim of a document in one call.
func ExampleNewSet() {
	var claims []hclext.Claim
	set, err := hclext.NewSet(hclext.Extension{
		Owner: "example.org/notes",
		Blocks: []hclext.Block{{Site: hclext.TopLevel, Type: "note", Naming: hclext.SchemaAndName,
			Attributes: []hclext.Attribute{{Name: "text", Kind: hclext.String}}}},
		Decoder: noteReader{claims: &claims},
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
		return
	}
	for _, claim := range claims {
		fmt.Printf("%s %s.%s: %q\n", claim.Block, claim.Schema, claim.Name, claim.Attributes["text"].Text)
	}
	// Output:
	// true
	// note app.welcome: "hello"
}

// ExampleTableAttributes registers attributes on a common table block. The
// frontend supplies evaluated values and the table identity in one claim.
func ExampleTableAttributes() {
	var claims []hclext.Claim
	set, err := hclext.NewSet(hclext.Extension{
		Owner: "example.org/notes",
		Blocks: []hclext.Block{{Site: hclext.TableAttributes, Type: "table",
			Attributes: []hclext.Attribute{{Name: "text", Kind: hclext.String}}}},
		Decoder: noteReader{claims: &claims},
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	shape, found := set.Lookup(hclext.TableAttributes, "table")
	fmt.Println(found)
	_, err = set.Decode(context.Background(), []hclext.Match{{Shape: shape, Claim: hclext.Claim{
		Block: "table", Schema: "app", Name: "cache",
		Table:      &hclext.Table{Schema: "app", Name: "cache", Struct: "app.cache"},
		Attributes: map[string]hclext.Value{"text": {Kind: hclext.String, Text: "temporary data"}},
	}}})
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, claim := range claims {
		fmt.Printf("%s %s.%s: %q\n", claim.Block, claim.Schema, claim.Name, claim.Attributes["text"].Text)
	}
	// Output:
	// true
	// table app.cache: "temporary data"
}
