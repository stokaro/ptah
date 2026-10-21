package pgdefaultprivilegesource

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/annotation"
	"ptah.run/core/coverage"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgdefaultprivilege"
)

// LimitKind names source directives that leave default privileges unmanaged.
// A name is a JSON tuple: grantor, schema, object class, and grantee for a grant;
// the first three components alone name an unreadable ACL class.
const LimitKind coverage.Kind = "default_privilege"

func sourceCoverage(limits []coverage.Object) (schemaext.Coverage, error) {
	knowledge := schemaext.Knowledge{State: schemaext.Complete}
	var subjects []schemaext.SubjectCoverage
	for _, limit := range limits {
		if limit.Kind != LimitKind {
			return schemaext.Coverage{}, fmt.Errorf("%w: unrelated default privilege source limit", schemaext.ErrInvalidValue)
		}
		unknown := schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the source leaves these default privileges unmanaged"}
		if limit.Name == "" {
			knowledge = unknown
			continue
		}
		ref, err := limitRef(limit.Name)
		if err != nil {
			return schemaext.Coverage{}, err
		}
		subjects = append(subjects, schemaext.SubjectCoverage{Kind: pgdefaultprivilege.DefaultPrivilegeKind, Subject: ref, Knowledge: unknown})
	}
	return pgdefaultprivilege.Coverage(schemaext.Desired, knowledge, subjects)
}

func limitRef(name string) (objectidentity.ID, error) {
	raw, err := schemaext.DecodeJSON[[]*string]([]byte(name))
	if err != nil {
		return objectidentity.ID{}, err
	}
	parts := make([]string, len(raw))
	for i, part := range raw {
		if part == nil {
			return objectidentity.ID{}, fmt.Errorf("%w: default privilege limit components must be strings", schemaext.ErrInvalidValue)
		}
		parts[i] = *part
	}
	semantics := identifier.ForDialect("postgres")
	switch len(parts) {
	case 3:
		return pgdefaultprivilege.ClassRef(pgdefaultprivilege.Class{Grantor: parts[0], Schema: parts[1], ObjectType: parts[2]}, semantics)
	case 4:
		return pgdefaultprivilege.Ref(pgdefaultprivilege.Identity{Grantor: parts[0], Schema: parts[1], ObjectType: parts[2], Grantee: parts[3]}, semantics)
	default:
		return objectidentity.ID{}, fmt.Errorf("%w: a default privilege limit requires a grantor, schema, object class and optional grantee", schemaext.ErrInvalidValue)
	}
}

func limitName(ref objectidentity.ID) (string, error) {
	var parts []string
	if ref.Kind == pgdefaultprivilege.ClassSubjectKind {
		class, err := pgdefaultprivilege.ClassOf(ref)
		if err != nil {
			return "", err
		}
		parts = []string{class.Grantor, class.Schema, class.ObjectType}
	} else {
		identity, err := pgdefaultprivilege.IdentityOf(ref)
		if err != nil {
			return "", err
		}
		parts = []string{identity.Grantor, identity.Schema, identity.ObjectType, identity.Grantee}
	}
	encoded, err := json.Marshal(parts)
	return string(encoded), err
}

func encodedLimits(known schemaext.Coverage) ([]annotation.EncodedLimit, error) {
	var result []annotation.EncodedLimit
	wholeUnknown := false
	for _, kind := range known.KindRecords() {
		if kind.Model.Kind != pgdefaultprivilege.DefaultPrivilegeKind {
			return nil, fmt.Errorf("%w: unrelated default privilege coverage model", schemaext.ErrInvalidValue)
		}
		if kind.Knowledge.HasNamespaceScopes() {
			return nil, fmt.Errorf("%w: this source format cannot preserve namespace coverage boundaries", ptaherr.ErrUnsupportedFeature)
		}
		wholeUnknown = kind.Knowledge.Unscoped().State != schemaext.Complete
		if wholeUnknown {
			result = append(result, annotation.EncodedLimit{Kind: string(LimitKind)})
		}
	}
	for _, claim := range known.SubjectRecords() {
		if err := pgdefaultprivilege.ValidateCoverageSubject(claim); err != nil {
			return nil, err
		}
		if len(claim.Targets) > 0 {
			return nil, fmt.Errorf("%w: a default privilege limit cannot preserve a target scope", ptaherr.ErrUnsupportedFeature)
		}
		switch claim.Knowledge.State {
		case schemaext.Complete:
			if wholeUnknown {
				return nil, fmt.Errorf("%w: a default privilege source cannot preserve a complete subject inside an unknown namespace", ptaherr.ErrUnsupportedFeature)
			}
		case schemaext.Uninspected, schemaext.Unrepresentable:
			name, err := limitName(claim.Subject)
			if err != nil {
				return nil, err
			}
			result = append(result, annotation.EncodedLimit{Kind: string(LimitKind), Name: name})
		default:
			return nil, fmt.Errorf("%w: a default privilege source cannot preserve an explicit %s claim", ptaherr.ErrUnsupportedFeature, claim.Knowledge.State)
		}
	}
	return result, nil
}
