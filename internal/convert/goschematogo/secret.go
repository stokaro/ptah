package goschematogo

import (
	"ptah.run/core/objectidentity"
	"ptah.run/dialect/ydb/ydbsecret"
)

// secretAnnotation writes a YDB secret as its annotation: its path and the
// variable its value comes from, and never a value, which no model holds. A
// declaration that names no variable is written with the default one for its
// path, which is what it selects.
func secretAnnotation(ref objectidentity.ID, secret *ydbsecret.Desired) (string, error) {
	if err := ydbsecret.ValidateIdentity(ref); err != nil {
		return "", err
	}
	if err := secret.Validate(); err != nil {
		return "", err
	}
	return annotation("ptah:schema:secret",
		attr{name: ydbsecret.AttributeName, value: ref.Name.Source, set: true},
		attr{name: ydbsecret.AttributeSchema, value: ref.Schema.Source, set: ref.Schema.Source != ""},
		attr{name: ydbsecret.AttributeValueEnv, value: secret.Variable(ref), set: true},
	), nil
}
