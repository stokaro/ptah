package atlasurl_test

import (
	"testing"
	_ "time/tzdata" // the zone the rows name, on a runner whose system has no zone database

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasurl"
)

// TestTextTimeLocation reads the zone from every form a MySQL-family URL has,
// and holds parseTime out of the answer: the driver writes in this zone whether
// or not the URL asks it to parse what it reads.
func TestTextTimeLocation(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want string
	}{
		{name: "no parameter", url: "mysql://app@db:3306/shop", want: "UTC"},
		{name: "parseTime alone", url: "mysql://app@db:3306/shop?parseTime=true", want: "UTC"},
		{name: "the URL form", url: "mysql://app@db:3306/shop?loc=Asia%2FTokyo", want: "Asia/Tokyo"},
		{
			name: "beside parseTime",
			url:  "mysql://app@db:3306/shop?parseTime=true&loc=Asia%2FTokyo",
			want: "Asia/Tokyo",
		},
		{name: "the driver's form", url: "mysql://app@tcp(db:3306)/shop?loc=Asia%2FTokyo", want: "Asia/Tokyo"},
		{
			name: "the socket form",
			url:  "mysql+unix://app@/run/mysqld/mysqld.sock?database=shop&loc=Asia%2FTokyo",
			want: "Asia/Tokyo",
		},
		{name: "the MariaDB scheme", url: "mariadb://app@db:3306/shop?loc=Asia%2FTokyo", want: "Asia/Tokyo"},
		{name: "another engine's URL", url: "postgres://app@db:5432/shop?loc=Asia%2FTokyo", want: "UTC"},
		{name: "a zone that does not exist", url: "mysql://app@db:3306/shop?loc=Not%2FAZone", want: "UTC"},
		{name: "no URL", url: "", want: "UTC"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(atlasurl.TextTimeLocation(test.url).String(), qt.Equals, test.want)
		})
	}
}
