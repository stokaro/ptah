// Package atlashash implements Atlas-compatible cumulative migration hashes.
// It is shared by migration-directory integrity files and revision-history
// verification so both layers use the same byte-level chain semantics.
package atlashash

import (
	"crypto/sha256"
	"encoding/base64"
	"hash"
	"strings"
)

const prefix = "h1:"

// Chain incrementally hashes Atlas migration source names and contents.
type Chain struct {
	hash hash.Hash
}

// NewChain returns an empty Atlas migration hash chain.
func NewChain() *Chain {
	return &Chain{hash: sha256.New()}
}

// Add appends a source name and its data to the chain and returns the
// cumulative Atlas h1 value.
func (c *Chain) Add(name string, data []byte) string {
	_, _ = c.hash.Write([]byte(name))
	_, _ = c.hash.Write(data)
	return prefix + base64.StdEncoding.EncodeToString(c.hash.Sum(nil))
}

// AddName appends only a source name and returns the cumulative Atlas h1 value.
// Atlas sum-ignore files use this path: their names remain part of the chain
// while their contents and entries do not.
func (c *Chain) AddName(name string) string {
	_, _ = c.hash.Write([]byte(name))
	return prefix + base64.StdEncoding.EncodeToString(c.hash.Sum(nil))
}

// IsSumIgnored reports whether data carries Atlas's sum-ignore directive.
func IsSumIgnored(data []byte) bool {
	content := string(data)
	prefix, rest, ok := strings.Cut(content, "atlas:")
	if !ok {
		return false
	}
	for _, r := range prefix {
		if r < ' ' || r > '~' {
			return false
		}
	}

	line, _, _ := strings.Cut(rest, "\n")
	nameEnd := 0
	for nameEnd < len(line) && isDirectiveNameChar(line[nameEnd]) {
		nameEnd++
	}
	if nameEnd == 0 {
		return false
	}
	args := ""
	if nameEnd < len(line) && line[nameEnd] == ' ' {
		args = strings.TrimLeft(line[nameEnd+1:], " ")
	}
	return line[:nameEnd] == "sum" && args == "ignore"
}

func isDirectiveNameChar(b byte) bool {
	return b == '_' || ('0' <= b && b <= '9') || ('A' <= b && b <= 'Z') || ('a' <= b && b <= 'z')
}
