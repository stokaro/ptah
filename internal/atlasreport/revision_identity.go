package atlasreport

import (
	"strconv"
	"strings"

	"ptah.run/migration/migrator"
)

// RevisionIdentityLabel is how an Atlas-facing message names a revision
// identity: as it is spelled, and the empty identity of a Flyway repeatable as
// `""`, so the sentence around it still reads.
func RevisionIdentityLabel(identity string) string {
	if identity == "" {
		return strconv.Quote(identity)
	}
	return identity
}

// NameRevisionIdentities rewrites message to name each migration by the exact
// revision identity its directory holds rather than by the execution-order key
// the run converted it to. A Flyway `V1.5__b.sql` executes under a large
// numeric key that appears in no file name and in no Atlas output, so a
// message naming that key names nothing the operator can find.
//
// identities maps each execution-order key to its identity. Only a whole run
// of digits that follows `migration `, `version `, `revision ` or `above ` and
// equals a key is rewritten. A number anywhere else, such as one inside a
// statement or a table name, is data and stays as written.
func NameRevisionIdentities(message string, identities map[int64]string) string {
	if len(identities) == 0 {
		return message
	}
	byKey := make(map[string]string, len(identities))
	for key, identity := range identities {
		byKey[strconv.FormatInt(key, 10)] = identity
	}
	var named strings.Builder
	for offset := 0; offset < len(message); {
		if !isDecimalDigit(message[offset]) {
			named.WriteByte(message[offset])
			offset++
			continue
		}
		end := offset + 1
		for end < len(message) && isDecimalDigit(message[end]) {
			end++
		}
		key := message[offset:end]
		if identity, ok := byKey[key]; ok && revisionLabelBefore(message[:offset]) {
			named.WriteString(RevisionIdentityLabel(identity))
		} else {
			named.WriteString(key)
		}
		offset = end
	}
	return named.String()
}

// MigrateApplyRevisionIdentities maps each execution-order key of a run to the
// revision identity it records: the identity the run selected the key under,
// and for every migration the directory holds, the identity it carries.
func MigrateApplyRevisionIdentities(
	versions []int64,
	keys []string,
	migrations []*migrator.Migration,
) map[int64]string {
	identities := make(map[int64]string, min(len(versions), len(keys))+len(migrations))
	for index, version := range versions {
		if index >= len(keys) {
			break
		}
		identities[version] = keys[index]
	}
	for _, migration := range migrations {
		if migration != nil {
			identities[migration.Version] = migration.RevisionVersion()
		}
	}
	return identities
}

func isDecimalDigit(b byte) bool {
	return b >= '0' && b <= '9'
}

// revisionLabelBefore reports whether prefix ends with a word that names a
// migration by its version.
func revisionLabelBefore(prefix string) bool {
	for _, label := range []string{"migration ", "version ", "revision ", "above "} {
		if strings.HasSuffix(prefix, label) {
			return true
		}
	}
	return false
}
