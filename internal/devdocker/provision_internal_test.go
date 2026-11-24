package devdocker

// White-box testing required: portFromDockerPort reads what `docker port`
// answered, and DockerCLI is its only caller. Seen from outside, the failure it
// classifies needs a container that exits between `docker run` and `docker
// port`, which a live daemon produces only when its timing allows, so the
// exported surface cannot reach the branch on demand.

import (
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestPortFromDockerPort_HappyPath reads the port out of each shape `docker
// port` prints: a loopback publish, and an all-interfaces one with its IPv6
// twin.
func TestPortFromDockerPort_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		out  string
	}{
		{name: "loopback", out: "127.0.0.1:32768\n"},
		{name: "all interfaces", out: "0.0.0.0:32768\n[::]:32768\n"},
		{name: "IPv6 first", out: "[::]:32768\n0.0.0.0:32768\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			port, err := portFromDockerPort("ptah-dev-x", "5432", []byte(test.out), nil)

			c.Assert(err, qt.IsNil)
			c.Assert(port, qt.Equals, "32768")
		})
	}
}

// TestPortFromDockerPort_FailurePath names the container and the cause, and
// wraps ErrPortNotRead, whether `docker port` failed, as it does for a
// container that has exited, or answered with no mapping.
func TestPortFromDockerPort_FailurePath(t *testing.T) {
	exit := errors.New("exit status 1")
	tests := []struct {
		name string
		out  string
		err  error
		want string
	}{
		{
			name: "the container has exited",
			out:  "no public port '5432/tcp' published for ptah-dev-x\n",
			err:  exit,
			want: "read published port of container ptah-dev-x: no public port '5432/tcp' published for ptah-dev-x: " +
				"exit status 1",
		},
		{
			name: "no mapping",
			out:  "\n",
			want: "read published port of container ptah-dev-x: it published none for 5432",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			port, err := portFromDockerPort("ptah-dev-x", "5432", []byte(test.out), test.err)

			c.Assert(err, qt.ErrorIs, ErrPortNotRead)
			c.Assert(err.Error(), qt.Equals, test.want)
			c.Assert(port, qt.Equals, "")
		})
	}
}
