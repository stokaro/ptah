package blankimportguard_test

import "os"

// writeFile is the fixture writer the self-test uses. It exists so the test
// body stays free of the error handling the declarative-tests rule forbids.
func writeFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o600)
}
