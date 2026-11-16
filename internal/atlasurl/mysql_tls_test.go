package atlasurl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasurl"
)

// TestParseMySQLURL_CABundle_HappyPath reads ssl-ca out of every TCP form. The
// driver is handed the rest of the query as written, and a URL renamed or
// rendered for display keeps the parameter, so a dev database derived from a
// URL is verified as the URL is.
//
// A CA path holds a "/", which go-sql-driver's own grammar cannot carry in a
// parameter: it finds the parameters after the last "/" of the DSN.
func TestParseMySQLURL_CABundle_HappyPath(t *testing.T) {
	tests := []struct {
		name        string
		rawURL      string
		wantCAFile  string
		wantDSN     string
		wantURL     string
		wantRenamed string
	}{
		// #nosec G101 -- fixture URL with a made-up password, not a credential
		{
			name:        "URL form with the path as written",
			rawURL:      "mysql://app:pw@db.internal:3306/shop?ssl-ca=/etc/mysql/ca.pem&parseTime=true",
			wantCAFile:  "/etc/mysql/ca.pem",
			wantDSN:     "app:pw@tcp(db.internal:3306)/shop?parseTime=true",
			wantURL:     "mysql://app:pw@db.internal:3306/shop?ssl-ca=/etc/mysql/ca.pem&parseTime=true",
			wantRenamed: "mysql://app:pw@db.internal:3306/other?ssl-ca=/etc/mysql/ca.pem&parseTime=true",
		},
		{
			name:        "URL form with the path escaped, beside tls=true",
			rawURL:      "mariadb://app@db.internal/shop?tls=true&ssl-ca=%2Fetc%2Fmysql%2Fca.pem",
			wantCAFile:  "/etc/mysql/ca.pem",
			wantDSN:     "app:@tcp(db.internal)/shop?tls=true",
			wantURL:     "mariadb://app@db.internal/shop?tls=true&ssl-ca=%2Fetc%2Fmysql%2Fca.pem",
			wantRenamed: "mariadb://app@db.internal/other?tls=true&ssl-ca=%2Fetc%2Fmysql%2Fca.pem",
		},
		// #nosec G101 -- fixture URL with a made-up password, not a credential
		{
			name:        "driver form with the path as written",
			rawURL:      "mysql://app:pw@tcp(db.internal:3306)/shop?ssl-ca=/etc/mysql/ca.pem&parseTime=true",
			wantCAFile:  "/etc/mysql/ca.pem",
			wantDSN:     "app:pw@tcp(db.internal:3306)/shop?parseTime=true",
			wantURL:     "mysql://app:pw@db.internal:3306/shop?parseTime=true&ssl-ca=%2Fetc%2Fmysql%2Fca.pem",
			wantRenamed: "mysql://app:pw@tcp(db.internal:3306)/other?parseTime=true&ssl-ca=%2Fetc%2Fmysql%2Fca.pem",
		},
		{
			name:        "driver form with a relative path and no other parameter",
			rawURL:      "maria://app@tcp(db.internal:3306)/shop?ssl-ca=certs/ca.pem",
			wantCAFile:  "certs/ca.pem",
			wantDSN:     "app@tcp(db.internal:3306)/shop",
			wantURL:     "maria://app@db.internal:3306/shop?ssl-ca=certs%2Fca.pem",
			wantRenamed: "maria://app@tcp(db.internal:3306)/other?ssl-ca=certs%2Fca.pem",
		},
		{
			name:        "driver form with a ? inside the path",
			rawURL:      "mysql://app@tcp(db.internal:3306)/shop?parseTime=true&ssl-ca=/etc/my?sql/ca.pem",
			wantCAFile:  "/etc/my?sql/ca.pem",
			wantDSN:     "app@tcp(db.internal:3306)/shop?parseTime=true",
			wantURL:     "mysql://app@db.internal:3306/shop?parseTime=true&ssl-ca=%2Fetc%2Fmy%3Fsql%2Fca.pem",
			wantRenamed: "mysql://app@tcp(db.internal:3306)/other?parseTime=true&ssl-ca=%2Fetc%2Fmy%3Fsql%2Fca.pem",
		},
		{
			name:        "a URL without ssl-ca carries none",
			rawURL:      "mysql://app@db.internal:3306/shop?tls=skip-verify",
			wantCAFile:  "",
			wantDSN:     "app:@tcp(db.internal:3306)/shop?tls=skip-verify",
			wantURL:     "mysql://app@db.internal:3306/shop?tls=skip-verify",
			wantRenamed: "mysql://app@db.internal:3306/other?tls=skip-verify",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := atlasurl.ParseMySQLURL(test.rawURL)

			c.Assert(err, qt.IsNil)
			c.Assert(got.CAFile(), qt.Equals, test.wantCAFile)
			c.Assert(got.DSN(), qt.Equals, test.wantDSN)
			c.Assert(got.URL().String(), qt.Equals, test.wantURL)
			renamed := got.WithDatabase("other")
			c.Assert(renamed, qt.Equals, test.wantRenamed)
			reparsed, err := atlasurl.ParseMySQLURL(renamed)
			c.Assert(err, qt.IsNil)
			c.Assert(reparsed.CAFile(), qt.Equals, test.wantCAFile)
			c.Assert(reparsed.Database(), qt.Equals, "other")
		})
	}
}

// TestParseMySQLURL_CABundle_FailurePath refuses a CA bundle the rest of the
// URL contradicts, and every ssl- parameter Ptah does not read, before any
// connection is attempted. go-sql-driver would send an unknown parameter to
// the server as a system variable, and a name with a hyphen is a syntax error
// there, so none of these URLs could connect today.
func TestParseMySQLURL_CABundle_FailurePath(t *testing.T) {
	const verified = `ssl-ca asks for TLS with a verified server certificate, and `
	tests := []struct {
		name    string
		rawURL  string
		wantErr string
	}{
		// #nosec G101 -- fixture URL with a made-up password, not a credential
		{
			name:    "tls=false",
			rawURL:  "mysql://app:Pa5wordX9@db:3306/shop?ssl-ca=/ca.pem&tls=false",
			wantErr: `invalid mysql URL: ` + verified + `tls=false turns TLS off; remove one of the two parameters`,
		},
		// #nosec G101 -- fixture URL with a made-up password, not a credential
		{
			name:    "tls=0, which the driver reads as false",
			rawURL:  "mysql://app:Pa5wordX9@db:3306/shop?tls=0&ssl-ca=/ca.pem",
			wantErr: `invalid mysql URL: ` + verified + `tls=false turns TLS off; .*`,
		},
		// #nosec G101 -- fixture URL with a made-up password, not a credential
		{
			name:    "tls=skip-verify",
			rawURL:  "mariadb://app:Pa5wordX9@db:3306/shop?ssl-ca=/ca.pem&tls=skip-verify",
			wantErr: `invalid mariadb URL: ` + verified + `tls=skip-verify does not verify the certificate; .*`,
		},
		{
			name:    "tls=preferred in the driver form",
			rawURL:  "mysql://app:Pa5wordX9@tcp(db:3306)/shop?tls=preferred&ssl-ca=/ca.pem",
			wantErr: `invalid mysql URL: ` + verified + `tls=preferred does not verify the certificate and continues unencrypted .*`,
		},
		// #nosec G101 -- fixture URL with a made-up password, not a credential
		{
			name:    "allowFallbackToPlaintext=true",
			rawURL:  "mysql://app:Pa5wordX9@db:3306/shop?ssl-ca=/ca.pem&allowFallbackToPlaintext=true",
			wantErr: `invalid mysql URL: ` + verified + `allowFallbackToPlaintext=true continues unencrypted .*`,
		},
		// #nosec G101 -- fixture URL with a made-up password, not a credential
		{
			name:    "an empty ssl-ca",
			rawURL:  "mysql://app:Pa5wordX9@db:3306/shop?ssl-ca=",
			wantErr: `invalid mysql URL: the ssl-ca parameter is empty; name a PEM file of CA certificates, as in \?ssl-ca=/etc/mysql/ca.pem`,
		},
		// #nosec G101 -- fixture URL with a made-up password, not a credential
		{
			name:    "ssl-ca given twice",
			rawURL:  "mysql://app:Pa5wordX9@db:3306/shop?ssl-ca=/a.pem&ssl-ca=/b.pem",
			wantErr: `invalid mysql URL: the ssl-ca parameter is given more than once; name one PEM file of CA certificates`,
		},
		// #nosec G101 -- fixture URL with a made-up password, not a credential
		{
			name:    "an ssl-ca that is not valid escaping",
			rawURL:  "mysql://app:Pa5wordX9@db:3306/shop?ssl-ca=/ca%zz.pem",
			wantErr: `invalid mysql URL: the ssl-ca parameter cannot be read as written; percent-encode a % in the path as %25`,
		},
		// #nosec G101 -- fixture URL with a made-up password, not a credential
		{
			name:    "a client certificate",
			rawURL:  "mysql://app:Pa5wordX9@db:3306/shop?ssl-ca=/ca.pem&ssl-cert=/client.pem",
			wantErr: `invalid mysql URL: the ssl-cert parameter is not supported; ssl-ca is the only ssl- parameter Ptah reads, and Ptah sends no client certificate, .*`,
		},
		{
			name:    "a client key in the driver form",
			rawURL:  "maria://app:Pa5wordX9@tcp(db:3306)/shop?ssl-key=/client-key.pem",
			wantErr: `invalid maria URL: the ssl-key parameter is not supported; .*`,
		},
		// #nosec G101 -- fixture URL with a made-up password, not a credential
		{
			name:    "a MySQL client option with no counterpart",
			rawURL:  "mysql://app:Pa5wordX9@db:3306/shop?ssl-mode=VERIFY_IDENTITY",
			wantErr: `invalid mysql URL: the ssl-mode parameter is not supported; .*`,
		},
		{
			name:    "the socket form",
			rawURL:  "mysql+unix://app:Pa5wordX9@/run/x.sock?database=shop&ssl-ca=/ca.pem",
			wantErr: `invalid mysql\+unix URL: ssl-ca verifies the server's certificate against the host the URL connects to, and a socket address names no host; connect over TCP to use ssl-ca`,
		},
		{
			name:    "the driver's socket form",
			rawURL:  "mariadb://app:Pa5wordX9@unix(/run/x.sock)/shop?ssl-ca=/ca.pem",
			wantErr: `invalid mariadb URL: ssl-ca verifies the server's certificate .*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := atlasurl.ParseMySQLURL(test.rawURL)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.Not(qt.ErrorMatches), `(?s).*(Pa5|wordX9).*`)
			c.Assert(got, qt.Equals, atlasurl.MySQLURL{})
		})
	}
}
