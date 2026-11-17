package testutils

import "strings"

// atlasCommunityBuildNotice opens the paragraph a source build of the
// community Atlas binary may append to an error, advising the official build.
const atlasCommunityBuildNotice = "You're running the community build of Atlas"

// WithoutAtlasCommunityBuildNotice returns output up to the notice a source
// build of the community Atlas binary may append to an error, so that a
// comparison reads the error rather than whether the notice was due. The
// notice is not part of the error and does not appear on every run: on a CI
// runner only the first failing invocation prints it, so whether a test sees
// it depends on which tests ran before it on the same machine. Output without
// the notice is returned unchanged.
func WithoutAtlasCommunityBuildNotice(output string) string {
	before, _, _ := strings.Cut(output, atlasCommunityBuildNotice)
	return before
}
