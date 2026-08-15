package migrationlintreport

// White-box testing required: sarifArtifactURI is unexported and its Windows
// answer is not reachable through the exported report on a Unix runner.

import (
	"net/url"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestSarifArtifactURI_NeverMakesADriveLetterAHostname pins the shape of an
// absolute-path artifact URI.
//
// url.URL.String() writes "//" before a Path that does not begin with one, so a
// Windows path slashed to "C:/a/b.sql" came out as "file://C:/a/b.sql" -- and
// every SARIF consumer parses that with authority "C:", turning the drive
// letter into a hostname. Unix paths already begin with a slash and were
// therefore correct by accident, which is why no runner noticed.
//
// The rows exercise the rendering directly rather than through
// sarifArtifactURI, because that function only reaches this branch for a path
// its own filepath.IsAbs accepts -- and a Windows drive path is not absolute on
// the machine this test usually runs on.
func TestSarifArtifactURI_NeverMakesADriveLetterAHostname(t *testing.T) {
	tests := []struct {
		name     string
		slashed  string
		wantURI  string
		wantHost string
	}{
		{
			name:    "a drive path keeps the drive in the path",
			slashed: "C:/a/b.sql",
			wantURI: "file:///C:/a/b.sql",
		},
		{
			name:    "an absolute unix path is unchanged",
			slashed: "/srv/app/b.sql",
			wantURI: "file:///srv/app/b.sql",
		},
		{
			name:    "a UNC-looking path does not gain a second slash",
			slashed: "/srv/share/b.sql",
			wantURI: "file:///srv/share/b.sql",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			uri := fileURI(test.slashed)
			parsed, err := url.Parse(uri)

			c.Assert(err, qt.IsNil)
			c.Assert(uri, qt.Equals, test.wantURI)
			c.Assert(parsed.Host, qt.Equals, test.wantHost)
		})
	}
}
