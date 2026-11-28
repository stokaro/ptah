// Package urlname names the target database URL and the dev database URL of a
// run in its diagnostics, the way the operator gave them.
//
// A command reads each URL from a flag, from the flag's PTAH_* variable, or
// from the project file, and an error that names a flag the operator never
// typed sends them to the wrong place. The zero [Names] names the URLs --url
// and --dev-url, the Atlas CLI's flags, which the compatibility surface reads;
// a native command passes its own flag names and says when the project file
// gave the dev URL. The sentences around the flag names are Ptah's own; the
// community binary does not print them.
package urlname

import "fmt"

// Source names where one URL of a run came from: a flag, or a key of the
// project file. The zero value names nothing; [Names] reads it as its default
// flag.
type Source struct {
	flag   string
	phrase string
}

// Flag is the source for a URL a flag or its PTAH_* variable set.
func Flag(name string) Source {
	return Source{flag: name}
}

// ProjectDev is the source for a dev URL the project file's dev key set.
func ProjectDev() Source {
	return Source{phrase: "the dev database named in the project file"}
}

// String is the noun phrase naming the URL: the flag, or a phrase for the
// project file, such as "the dev database named in the project file".
func (s Source) String() string {
	if s.phrase != "" {
		return s.phrase
	}
	return s.flag
}

// Database is the noun phrase for the database the URL names: `the --dev-url
// database` for a flag, and the phrase itself, which names a database, for
// the project file.
func (s Source) Database() string {
	if s.phrase != "" {
		return s.phrase
	}
	return "the " + s.flag + " database"
}

// Attribute names an attribute of the URL and its value. A flag reads as a
// label and goes first, `--dev-url dialect "postgres"`; a phrase does not, so
// the attribute leads, `the dialect "postgres" of the dev database named in
// the project file`.
func (s Source) Attribute(attribute, value string) string {
	if s.phrase != "" {
		return fmt.Sprintf("the %s %q of %s", attribute, value, s.phrase)
	}
	return fmt.Sprintf("%s %s %q", s.flag, attribute, value)
}

// Or is s, or fallback where s is the zero value.
func (s Source) Or(fallback Source) Source {
	if s == (Source{}) {
		return fallback
	}
	return s
}

// Names says how a run's diagnostics name its target and dev database URLs.
// The zero value names them --url and --dev-url.
type Names struct {
	// Target is where the target database URL came from. The zero value is
	// --url.
	Target Source
	// Dev is where the dev database URL came from. The zero value is
	// --dev-url.
	Dev Source
}

// TargetSource is [Names.Target], or --url where it is the zero value.
func (n Names) TargetSource() Source {
	return n.Target.Or(Flag("--url"))
}

// DevSource is [Names.Dev], or --dev-url where it is the zero value.
func (n Names) DevSource() Source {
	return n.Dev.Or(Flag("--dev-url"))
}

// DialectMismatch is the refusal of a dev database of devDialect for a target
// of targetDialect. The zero value names the compatibility surface's flags,
// `--dev-url dialect "x" does not match --url dialect "y"`.
func (n Names) DialectMismatch(devDialect, targetDialect string) error {
	return fmt.Errorf("%s does not match %s",
		n.DevSource().Attribute("dialect", devDialect), n.TargetSource().Attribute("dialect", targetDialect))
}
