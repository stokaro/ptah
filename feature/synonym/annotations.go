package synonym

import (
	"ptah.run/core/annotation"
	"ptah.run/core/schemaext"
)

// Directive declares a synonym in a Go annotation.
const Directive = "ptah:schema:synonym"

// Annotations is the owner's contribution to the Go annotation frontend: the
// directive, its decoder, and the claim that a Go annotation source describes
// every synonym, so a synonym the source leaves out is dropped.
//
// There is no dialect attribute: a synonym is bound to the targets that have
// one, and every other target leaves it out.
func Annotations() annotation.Extension {
	return annotation.Extension{
		Owner:      Owner,
		Directives: []annotation.Directive{directive()},
		Kinds:      []schemaext.Kind{Kind},
		Decode:     decode,
		Coverage: annotation.Unlimited(func() (schemaext.Coverage, error) {
			return Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)
		}),
	}
}

func decode(declaration annotation.Declaration) ([]annotation.Contribution, error) {
	kv := declaration.Attributes
	synonym := DesiredSynonym{StructName: declaration.Struct, Comment: kv["comment"], Synonym: Synonym{
		Schema: kv["schema"], Name: kv["name"], Target: kv["target"],
	}}
	if err := Validate(synonym.Synonym); err != nil {
		return nil, err
	}
	object := DeclaredObject(synonym)
	return []annotation.Contribution{{Object: &object, Label: "synonym " + synonym.QualifiedName()}}, nil
}

func directive() annotation.Directive {
	return annotation.Directive{
		Name:        Directive,
		Description: "Declares a synonym, an alias for another object, on SQL Server and Oracle.",
		Scopes:      []annotation.Scope{annotation.ScopeStruct},
		Attributes: []annotation.Attribute{
			{Name: "name", Description: "Synonym name, the alias being declared.", Value: "string", Required: true},
			{Name: "schema", Description: "Schema the alias lives in.", Value: "string"},
			{Name: "target", Description: "Object the alias stands for, as one to four dot-separated parts.", Value: "string", Required: true},
			{Name: "comment", Description: "Synonym comment.", Value: "string"},
		},
	}
}
