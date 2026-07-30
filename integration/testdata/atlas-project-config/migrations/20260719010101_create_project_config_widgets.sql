-- atlas:txmode none

CREATE INDEX CONCURRENTLY idx_ptah_issue_276_seed_id ON ptah_issue_276_seed (id);
SELECT pg_sleep(0.05);
CREATE TABLE ptah_issue_276_widgets (id INT PRIMARY KEY);
