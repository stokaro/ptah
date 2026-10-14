package pgdefaultprivilegesource

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"go.yaml.in/yaml/v3"

	"ptah.run/core/schemaext"
	"ptah.run/core/yamlext"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/internal/dialectscope"
	"ptah.run/internal/yamlvalue"
)

// YAML registers the default_privileges section. Mapping keys are diagnostic
// labels; grantor, schema, object class, and grantee identify each ACL.
func YAML() yamlext.Extension {
	return yamlext.Extension{Owner: pgdefaultprivilege.Owner, Kinds: []schemaext.Kind{pgdefaultprivilege.DefaultPrivilegeKind},
		Sections: []yamlext.Section{{Key: Key, Decode: decodeYAML}}, Coverage: completeDesired}
}

type yamlDeclaration struct {
	ForRole    yamlvalue.Scalar `yaml:"for_role"`
	Schema     yamlvalue.Scalar `yaml:"schema"`
	ObjectType yamlvalue.Scalar `yaml:"object_type"`
	Grantee    yamlvalue.Scalar `yaml:"grantee"`
	Privileges yamlvalue.List   `yaml:"privileges"`
	Grantable  yamlvalue.List   `yaml:"grantable"`
	Revoked    yamlvalue.List   `yaml:"revoked"`
	Comment    yamlvalue.Scalar `yaml:"comment"`
	// A present empty value is an invalid scope, while absence is unrestricted.
	Dialects yaml.Node `yaml:"dialects"`
}

func decodeYAML(decode func(any) error, _ yamlext.Tables) ([]yamlext.Contribution, error) {
	var entries map[string]yamlDeclaration
	if err := decode(&entries); err != nil {
		return nil, err
	}
	var objects schemaext.Objects
	for _, label := range slices.Sorted(maps.Keys(entries)) {
		object, err := yamlObject(entries[label])
		if err != nil {
			return nil, fmt.Errorf("%s.%s: %w", Key, label, err)
		}
		objects, err = objects.Declare(object)
		if err != nil {
			return nil, fmt.Errorf("%s.%s: %w", Key, label, err)
		}
	}
	all, err := objects.All()
	if err != nil {
		return nil, err
	}
	result := make([]yamlext.Contribution, 0, len(all))
	for _, object := range all {
		result = append(result, yamlext.Contribution{Object: &object, Label: "default privilege " + object.Ref.String()})
	}
	return result, nil
}

func yamlObject(entry yamlDeclaration) (schemaext.Object, error) {
	var targets []string
	if entry.Dialects.Kind != 0 {
		var names yamlvalue.List
		if err := entry.Dialects.Decode(&names); err != nil {
			return schemaext.Object{}, err
		}
		var err error
		targets, err = dialectscope.Parse(strings.Join(names, ","))
		if err != nil {
			return schemaext.Object{}, fmt.Errorf("dialects: %w", err)
		}
	}
	return Declare(pgdefaultprivilege.Identity{Grantor: string(entry.ForRole), Schema: string(entry.Schema), ObjectType: string(entry.ObjectType), Grantee: string(entry.Grantee)},
		entry.Privileges, entry.Grantable, entry.Revoked, string(entry.Comment), targets)
}
