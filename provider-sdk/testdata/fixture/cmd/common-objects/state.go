package main

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"slices"
)

// state is what the program keeps beside a database: the roles, the grants
// and the comments SQLite has no place for.
type state struct {
	Roles    []stateRole    `json:"roles,omitempty"`
	Grants   []stateGrant   `json:"grants,omitempty"`
	Comments []stateComment `json:"comments,omitempty"`
}

type stateRole struct {
	Name     string   `json:"name"`
	Login    bool     `json:"login,omitempty"`
	Group    bool     `json:"group,omitempty"`
	MemberOf []string `json:"member_of,omitempty"`
}

// stateGrant is one privilege a role holds on one table or view.
type stateGrant struct {
	Role       string   `json:"role"`
	Privilege  string   `json:"privilege"`
	Kind       string   `json:"kind"`
	Name       string   `json:"name"`
	Columns    []string `json:"columns,omitempty"`
	WithOption bool     `json:"with_option,omitempty"`
}

// stateComment is the comment of one object: a table, a column of one, a
// view, or a role.
type stateComment struct {
	Kind  string `json:"kind"`
	Table string `json:"table,omitempty"`
	Name  string `json:"name"`
	Text  string `json:"text"`
}

func statePath(database string) string { return database + ".common.json" }

func loadState(database string) (state, error) {
	data, err := os.ReadFile(statePath(database)) // #nosec G304 -- the path is the session's own database file
	if errors.Is(err, fs.ErrNotExist) {
		return state{}, nil
	}
	if err != nil {
		return state{}, err
	}
	var loaded state
	return loaded, json.Unmarshal(data, &loaded)
}

func saveState(database string, saved state) error {
	data, err := json.Marshal(saved)
	if err != nil {
		return err
	}
	return os.WriteFile(statePath(database), data, 0o600)
}

func (s *state) role(name string) int {
	return slices.IndexFunc(s.Roles, func(role stateRole) bool { return role.Name == name })
}

// setComment replaces the comment of an object, and removes it for empty
// text.
func (s *state) setComment(comment stateComment) {
	s.Comments = slices.DeleteFunc(s.Comments, func(held stateComment) bool {
		return held.Kind == comment.Kind && held.Table == comment.Table && held.Name == comment.Name
	})
	if comment.Text != "" {
		s.Comments = append(s.Comments, comment)
	}
}
