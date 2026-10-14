package hclext_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/hclext"
	"ptah.run/core/schemaext"
)

func unlabeledExtension(block hclext.EncodedBlock) hclext.Extension {
	return hclext.Extension{Owner: "example.org/a", Kinds: []schemaext.Kind{"example.org/a/x"},
		Blocks: []hclext.Block{{Site: hclext.TopLevel, Type: "rule", Naming: hclext.Unlabeled,
			Attributes: []hclext.Attribute{{Name: "words", Kind: hclext.TextList}, {Name: "to", Kind: hclext.Ref, Refers: "role"}}}},
		Decoder: answeringDecoder{}, Encoder: answeringEncoder{result: hclext.EncodeResult{Blocks: []hclext.EncodedBlock{block}}}}
}

func TestUnlabeled_HappyPath(t *testing.T) {
	c := qt.New(t)
	block := hclext.EncodedBlock{Type: "rule", Unlabeled: true, Placement: hclext.AfterTriggers,
		Attributes: []hclext.EncodedAttribute{
			{Name: "words", Value: hclext.Value{Kind: hclext.TextList, Texts: []string{"first", "second"}}},
			{Name: "to", Value: hclext.Value{Kind: hclext.Ref, Reference: &hclext.Reference{Kind: "role", Name: "app.reader"}}},
		}}
	set, err := hclext.NewSet(unlabeledExtension(block))
	c.Assert(err, qt.IsNil)
	result, err := set.Encode(t.Context(), hclext.EncodeRequest{})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Blocks, qt.DeepEquals, []hclext.EncodedBlock{block})
}

func TestUnlabeled_RefusesATypeDeclaration(t *testing.T) {
	c := qt.New(t)
	extension := unlabeledExtension(hclext.EncodedBlock{})
	extension.Blocks[0].DeclaresType = true
	set, err := hclext.NewSet(extension)
	c.Assert(err, qt.ErrorMatches, `.*cannot declare a type without labels`)
	c.Assert(set.Selected(), qt.IsFalse)
}

func TestUnlabeled_RefusesInvalidEncoding(t *testing.T) {
	tests := []struct {
		name    string
		block   hclext.EncodedBlock
		wantErr string
	}{
		{name: "name with unlabeled", block: hclext.EncodedBlock{Type: "rule", Unlabeled: true, Name: "named", Placement: hclext.AfterTriggers}, wantErr: "labels that disagree"},
		{name: "named encoding of unlabeled shape", block: hclext.EncodedBlock{Type: "rule", Name: "named", Placement: hclext.AfterTriggers}, wantErr: "labels that disagree"},
		{name: "no reader", block: hclext.EncodedBlock{Type: "other", Unlabeled: true, Placement: hclext.AfterTriggers}, wantErr: "labels that disagree"},
		{name: "in table", block: hclext.EncodedBlock{Type: "rule", Unlabeled: true, Placement: hclext.WithinTable, Table: &hclext.Table{Name: "docs"}}, wantErr: "in a table without the table or with a name"},
		{name: "qualified role", block: hclext.EncodedBlock{Type: "rule", Unlabeled: true, Placement: hclext.AfterTriggers,
			Attributes: []hclext.EncodedAttribute{{Name: "to", Value: hclext.Value{Kind: hclext.Ref, Reference: &hclext.Reference{Kind: "role", Schema: "app", Name: "reader"}}}}}, wantErr: "writer cannot write"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			set := must.Must(hclext.NewSet(unlabeledExtension(test.block)))
			result, err := set.Encode(t.Context(), hclext.EncodeRequest{})
			c.Assert(err, qt.ErrorMatches, "(?s).*"+test.wantErr+".*")
			c.Assert(result, qt.DeepEquals, hclext.EncodeResult{})
		})
	}
}
