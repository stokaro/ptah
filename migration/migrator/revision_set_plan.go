package migrator

// planRevisionSet makes the same identity and retirement decisions for SQL and
// provider storage. Storage adapters apply this plan within a serializable read.
func (m *Migrator) planRevisionSet(existing []MigrationRevision, migrations []*Migration, version int64) (AtlasRevisionSetResult, []string, error) {
	if err := m.refuseRespelledRevisions(existing); err != nil {
		return AtlasRevisionSetResult{}, nil, err
	}
	var retired, exactRemoved []string
	if m.revisionTableFormat.isAtlas() {
		retired = m.retiredAtlasRevisionVersions()
		var err error
		exactRemoved, err = m.unownedExactAtlasRevisionsAbove(existing, setBoundary(migrations))
		if err != nil {
			return AtlasRevisionSetResult{}, nil, err
		}
		exactRemoved = append(exactRemoved, m.unnumberedRemovedAtlasRevisions(existing, migrations, version, retired, exactRemoved)...)
	}
	return m.atlasRevisionSetChanges(existing, migrations, version, retired, exactRemoved), exactRemoved, nil
}

func (m *Migrator) findSetRevision(index setRevisionIndex, migration *Migration) (MigrationRevision, bool) {
	if m.revisionTableFormat.isAtlas() {
		return index.find(migration)
	}
	// Native history stores the numeric order key even when the source uses an
	// exact Atlas identity. Reporting must use the same key as the write path.
	revision, exists := index.byVersion[migration.Version]
	return revision, exists
}
