package builtin

import (
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbworkload"
)

// refuseResourcePool refuses subject, a resource pool or a classifier, on a
// target without [capability.ResourcePools]: a pool is YDB's, and a target
// that built nothing for it would report the declaration applied. On YDB the
// refusal says how the cluster turns the key on.
func refuseResourcePool(dialect string, caps capability.Capabilities, subject string) error {
	if caps.Has(capability.ResourcePools) {
		return nil
	}
	normalized := platform.NormalizeDialect(dialect)
	message := fmt.Sprintf("%s, which requires target capability %s, unavailable on this %s target",
		subject, capability.ResourcePools, normalized)
	if normalized == platform.YDB {
		message += "; " + ydbworkload.FlagHint
	}
	return &ptaherr.CapabilityError{
		Dialect: normalized,
		Feature: string(capability.ResourcePools),
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: message,
	}
}

// validateDeclaredResourcePools refuses a declared pool or classifier the
// target cannot create, before any statement is emitted: on a target without
// [capability.ResourcePools], a pool or classifier YDB refuses on every line,
// and a set of classifiers that cannot stand together -- two on one rank, or
// one naming a pool nobody declared.
func validateDeclaredResourcePools(dialect string, caps capability.Capabilities, database *schemamodel.Database) error {
	pools := make([]string, 0, len(database.ResourcePools))
	for _, pool := range database.ResourcePools {
		if err := refuseResourcePool(dialect, caps, fmt.Sprintf("resource pool %q", pool.Name)); err != nil {
			return err
		}
		if err := resourcePoolRefusal(dialect, ydbworkload.CheckPool(pool.Name, pool.Spec, caps)); err != nil {
			return err
		}
		pools = append(pools, pool.Name)
	}
	classifiers := make([]ydbworkload.Classifier, 0, len(database.ResourcePoolClassifiers))
	for _, classifier := range database.ResourcePoolClassifiers {
		subject := fmt.Sprintf("resource pool classifier %q", classifier.Name)
		if err := refuseResourcePool(dialect, caps, subject); err != nil {
			return err
		}
		if err := resourcePoolRefusal(dialect,
			ydbworkload.CheckClassifier(classifier.Name, classifier.Spec, caps)); err != nil {
			return err
		}
		classifiers = append(classifiers, ydbworkload.Classifier{Name: classifier.Name, Spec: classifier.Spec})
	}
	return resourcePoolRefusal(dialect, ydbworkload.CheckRouting(pools, classifiers))
}

// resourcePoolRefusal turns a refusal YDB makes on every line into the
// renderer's error. A refusal through the key never reaches it: the key is
// checked first.
func resourcePoolRefusal(dialect string, refusal *ydbworkload.Refusal) error {
	if refusal == nil {
		return nil
	}
	return &ptaherr.RenderError{
		Dialect: platform.NormalizeDialect(dialect),
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf("%s: %s", refusal.Subject, refusal.Reason),
	}
}
