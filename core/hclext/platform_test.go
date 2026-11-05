package hclext_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/hclext"
	"ptah.run/core/schemaext"
)

// platform is a table platform group of label with one string override,
// read under aliases too.
func platform(label string, aliases ...string) hclext.Block {
	return hclext.Block{Site: hclext.TablePlatform, Type: label, Aliases: aliases, Attributes: []hclext.Attribute{{Name: "ttl", Kind: hclext.String}}}
}

// platformExtension owns the platform blocks and encodes with encoder.
func platformExtension(owner string, kind schemaext.Kind, encoder hclext.Encoder, blocks ...hclext.Block) hclext.Extension {
	extension := extension(owner, kind, answeringDecoder{}, blocks...)
	extension.Encoder = encoder
	return extension
}

// TestSet_LookupTablePlatform finds a platform group under its type and each
// alias, as the type's shape, and no group under another label.
func TestSet_LookupTablePlatform(t *testing.T) {
	c := qt.New(t)
	set := must.Must(hclext.NewSet(extension("example.org/a", "example.org/a/x", answeringDecoder{}, platform("ydb", "ydbs"))))

	byType, typed := set.Lookup(hclext.TablePlatform, "ydb")
	byAlias, aliased := set.Lookup(hclext.TablePlatform, "ydbs")
	_, other := set.Lookup(hclext.TablePlatform, "mysql")
	_, inTable := set.Lookup(hclext.InTable, "ydb")

	c.Assert(typed, qt.IsTrue)
	c.Assert(aliased, qt.IsTrue)
	c.Assert(byType.Block, qt.DeepEquals, platform("ydb", "ydbs"))
	c.Assert(byAlias.Block, qt.DeepEquals, platform("ydb", "ydbs"))
	c.Assert(other, qt.IsFalse)
	c.Assert(inTable, qt.IsFalse)
}

func TestNewSet_TablePlatform_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name       string
		extensions []hclext.Extension
		want       string
	}{
		{"a naming", []hclext.Extension{extension("example.org/a", "example.org/a/x", answeringDecoder{},
			hclext.Block{Site: hclext.TablePlatform, Type: "ydb", Naming: hclext.Unlabeled})},
			`HCL table-platform block "ydb" of example.org/a takes no naming, and its overrides are strings`},
		{"an override that is no string", []hclext.Extension{extension("example.org/a", "example.org/a/x", answeringDecoder{},
			hclext.Block{Site: hclext.TablePlatform, Type: "ydb", Attributes: []hclext.Attribute{{Name: "ttl", Kind: hclext.Text}}})},
			`HCL table-platform block "ydb" of example.org/a takes no naming, and its overrides are strings`},
		{"a child", []hclext.Extension{extension("example.org/a", "example.org/a/x", answeringDecoder{},
			hclext.Block{Site: hclext.TablePlatform, Type: "ydb", Children: []hclext.Child{{Type: "tier"}}})},
			`HCL table-platform block "ydb" of example.org/a declares a type outside top level or children outside top level and in-table sites`},
		{"aliases at another site", []hclext.Extension{extension("example.org/a", "example.org/a/x", answeringDecoder{},
			hclext.Block{Site: hclext.InTable, Type: "ydb", Aliases: []string{"ydbs"}})},
			`HCL in-table block "ydb" of example.org/a has aliases, which only a table-platform block takes`},
		{"an empty alias", []hclext.Extension{extension("example.org/a", "example.org/a/x", answeringDecoder{}, platform("ydb", " "))},
			`HCL table-platform block "ydb" of example.org/a has an alias that is empty, written twice or its own type`},
		{"an alias written twice", []hclext.Extension{extension("example.org/a", "example.org/a/x", answeringDecoder{}, platform("ydb", "ydbs", "ydbs"))},
			`HCL table-platform block "ydb" of example.org/a has an alias that is empty, written twice or its own type`},
		{"an alias of its own type", []hclext.Extension{extension("example.org/a", "example.org/a/x", answeringDecoder{}, platform("ydb", "ydb"))},
			`HCL table-platform block "ydb" of example.org/a has an alias that is empty, written twice or its own type`},
		{"one label two owners read", []hclext.Extension{extension("example.org/a", "example.org/a/x", answeringDecoder{}, platform("ydb")),
			extension("example.org/b", "example.org/b/x", answeringDecoder{}, platform("other", "ydb"))},
			`duplicate feature identity: HCL table-platform block "ydb" is read by example.org/a and example.org/b`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			set, err := hclext.NewSet(test.extensions...)

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(set.Selected(), qt.IsFalse)
		})
	}
}

// TestSet_Decode_TellsTheOwnerTheSite hands a decoder each claim with the
// site of the shape that matched it, so a platform group of type ydb is told
// from a block of type ydb in a table.
func TestSet_Decode_TellsTheOwnerTheSite(t *testing.T) {
	c := qt.New(t)
	var requests []hclext.Request
	inTable := hclext.Block{Site: hclext.InTable, Type: "ydb"}
	set := must.Must(hclext.NewSet(extension("example.org/a", "example.org/a/x", recordingDecoder{kind: "example.org/a/x", requests: &requests},
		platform("ydb"), inTable)))
	group, _ := set.Lookup(hclext.TablePlatform, "ydb")
	block, _ := set.Lookup(hclext.InTable, "ydb")

	_, err := set.Decode(t.Context(), []hclext.Match{{Shape: group, Claim: hclext.Claim{Block: "ydb", Name: "g"}}, {Shape: block, Claim: hclext.Claim{Block: "ydb", Name: "b"}}})

	c.Assert(err, qt.IsNil)
	c.Assert(requests, qt.DeepEquals, []hclext.Request{{Claims: []hclext.Claim{{Site: hclext.TablePlatform, Block: "ydb", Name: "g"}, {Site: hclext.InTable, Block: "ydb", Name: "b"}}}})
}

// TestSet_Encode_WritesPlatformOverrides returns the overrides an owner
// writes in a table's platform group of its own.
func TestSet_Encode_WritesPlatformOverrides(t *testing.T) {
	c := qt.New(t)
	table := slotDocument().Tables[0].Table
	written := []hclext.SlotAttributes{{Slot: hclext.Slot{Kind: hclext.SlotTable, Table: table},
		Overrides: []hclext.PlatformOverride{{Platform: "ydb", Key: "ttl", Value: "P1D"}}, Losses: []string{"the partitioning is not represented in HCL"}}}
	set := must.Must(hclext.NewSet(platformExtension("example.org/a", "example.org/a/x", slotEncoder{slots: written}, platform("ydb"))))

	encoded, err := set.Encode(t.Context(), slotDocument())

	c.Assert(err, qt.IsNil)
	c.Assert(encoded.Attributes, qt.DeepEquals, written)
}

func TestSet_Encode_PlatformOverrides_FailurePath(t *testing.T) {
	table := slotDocument().Tables[0].Table
	override := func(platform, key string) []hclext.PlatformOverride {
		return []hclext.PlatformOverride{{Platform: platform, Key: key, Value: "P1D"}}
	}
	for _, test := range []struct {
		name  string
		slots []hclext.SlotAttributes
		want  string
	}{
		{"an override of an index", []hclext.SlotAttributes{{Slot: hclext.Slot{Kind: hclext.SlotIndex, Table: table, Name: "i"}, Overrides: override("ydb", "ttl")}},
			`invalid feature value: HCL encoder of example.org/a wrote platform "ydb" override "ttl" of index "i" of table "app.t", which no platform block of its own takes there, or twice`},
		{"an override of another group", []hclext.SlotAttributes{{Slot: hclext.Slot{Kind: hclext.SlotTable, Table: table}, Overrides: override("mysql", "ttl")}},
			`invalid feature value: HCL encoder of example.org/a wrote platform "mysql" override "ttl" of table "app.t", which no platform block of its own takes there, or twice`},
		{"an override the group does not take", []hclext.SlotAttributes{{Slot: hclext.Slot{Kind: hclext.SlotTable, Table: table}, Overrides: override("ydb", "unit")}},
			`invalid feature value: HCL encoder of example.org/a wrote platform "ydb" override "unit" of table "app.t", which no platform block of its own takes there, or twice`},
		{"an override under an alias", []hclext.SlotAttributes{{Slot: hclext.Slot{Kind: hclext.SlotTable, Table: table}, Overrides: override("ydbs", "ttl")}},
			`invalid feature value: HCL encoder of example.org/a wrote platform "ydbs" override "ttl" of table "app.t", which no platform block of its own takes there, or twice`},
		{"an override twice", []hclext.SlotAttributes{{Slot: hclext.Slot{Kind: hclext.SlotTable, Table: table}, Overrides: append(override("ydb", "ttl"), override("ydb", "ttl")...)}},
			`invalid feature value: HCL encoder of example.org/a wrote platform "ydb" override "ttl" of table "app.t", which no platform block of its own takes there, or twice`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			set := must.Must(hclext.NewSet(platformExtension("example.org/a", "example.org/a/x", slotEncoder{slots: test.slots}, platform("ydb", "ydbs"))))

			encoded, err := set.Encode(t.Context(), slotDocument())

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(encoded, qt.DeepEquals, hclext.EncodeResult{})
		})
	}
}

// TestSet_Encode_RefusesAnOverrideOfAnotherOwnersGroup refuses an extension
// writing an override of a group another extension reads, so two extensions
// never write one override.
func TestSet_Encode_RefusesAnOverrideOfAnotherOwnersGroup(t *testing.T) {
	c := qt.New(t)
	table := hclext.Slot{Kind: hclext.SlotTable, Table: slotDocument().Tables[0].Table}
	write := slotEncoder{slots: []hclext.SlotAttributes{{Slot: table, Overrides: []hclext.PlatformOverride{{Platform: "ydb", Key: "ttl", Value: "P1D"}}}}}
	first := platformExtension("example.org/a", "example.org/a/x", write, platform("ydb"))
	second := platformExtension("example.org/b", "example.org/b/x", write)
	second.Blocks = []hclext.Block{{Site: hclext.TopLevel, Type: "other", Naming: hclext.Unlabeled}}
	set := must.Must(hclext.NewSet(first, second))

	encoded, err := set.Encode(t.Context(), slotDocument())

	c.Assert(err, qt.ErrorMatches, `invalid feature value: HCL encoder of example.org/b wrote platform "ydb" override "ttl" of table "app.t", which no platform block of its own takes there, or twice`)
	c.Assert(encoded, qt.DeepEquals, hclext.EncodeResult{})
}
