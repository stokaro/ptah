package atlas

import (
	"strconv"
	"strings"

	"go.5x5.cz/ptah/migration/migrator"
)

type atlasExactIdentityError struct {
	cause   error
	message string
}

func (e *atlasExactIdentityError) Error() string { return e.message }

func (e *atlasExactIdentityError) Unwrap() error { return e.cause }

func remapAtlasExactIdentityError(err error, identities map[int64]string) error {
	if err == nil || len(identities) == 0 {
		return err
	}
	identityByRuntime := make(map[string]string, len(identities))
	for runtime, identity := range identities {
		key := strconv.FormatInt(runtime, 10)
		identityByRuntime[key] = identity
	}
	message := replaceAtlasRuntimeRuns(err.Error(), identityByRuntime)
	if message == err.Error() {
		return err
	}
	return &atlasExactIdentityError{cause: err, message: message}
}

func replaceAtlasRuntimeRuns(message string, identities map[string]string) string {
	var replaced strings.Builder
	for offset := 0; offset < len(message); {
		if message[offset] < '0' || message[offset] > '9' {
			replaced.WriteByte(message[offset])
			offset++
			continue
		}
		end := offset + 1
		for end < len(message) && message[end] >= '0' && message[end] <= '9' {
			end++
		}
		runtime := message[offset:end]
		if identity, ok := identities[runtime]; ok && atlasRuntimeLabelBefore(message[:offset]) {
			replaced.WriteString(atlasExactIdentityLabel(identity))
		} else {
			replaced.WriteString(runtime)
		}
		offset = end
	}
	return replaced.String()
}

func atlasExactIdentityLabel(identity string) string {
	if identity == "" {
		return strconv.Quote(identity)
	}
	return identity
}

func atlasRuntimeLabelBefore(prefix string) bool {
	for _, label := range []string{"migration ", "version ", "revision ", "above "} {
		if strings.HasSuffix(prefix, label) {
			return true
		}
	}
	return false
}

func atlasMigrateApplyRevisionIdentities(
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

var _ interface{ Unwrap() error } = (*atlasExactIdentityError)(nil)
var _ error = (*atlasExactIdentityError)(nil)
