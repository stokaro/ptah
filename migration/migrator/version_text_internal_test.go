package migrator

// White-box testing required: versionText is unexported, and its two branches
// are observable from the exported API only through the messages that print
// it, where a numbered file and a numberless one each reach one branch. A
// table over the verbs is the only place both branches meet every verb.

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestVersionTextFormat_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		format string
		label  string
		want   string
	}{
		{name: "d number", format: "%d", want: "3"},
		{name: "v number", format: "%v", want: "3"},
		{name: "s number", format: "%s", want: "%!s(int64=3)"},
		{name: "width number", format: "%5d", want: "    3"},
		{name: "q number", format: "%q", want: `'\x03'`},
		{name: "d label", format: "%d", label: "v1", want: "v1"},
		{name: "v label", format: "%v", label: "v1", want: "v1"},
		{name: "s label", format: "%s", label: "v1", want: "v1"},
		{name: "width label", format: "%5d", label: "v1", want: "v1"},
		{name: "q label", format: "%q", label: "v1", want: "v1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(fmt.Sprintf(tt.format, versionText{number: 3, label: tt.label}), qt.Equals, tt.want)
		})
	}
}
