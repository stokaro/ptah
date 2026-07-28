env "development" {
  url = "postgres://app@localhost:5432/development?sslmode=disable"
  dev = "docker://postgres/16/development"
  migration {
    dir = "file://development-migrations"
  }
  lint {
    latest = 2
  }
}

env "production" {
  url = "postgres://app@localhost:5432/production?sslmode=disable"
  dev = "docker://postgres/16/production"
  src = ["file://production.hcl"]
  exclude = ["audit_*"]
  migration {
    dir              = "file://production-migrations"
    revisions_schema = "migration_meta"
    lock_timeout     = "10s"
    exec_order       = "linear-skip"
  }
  lint {
    latest = 8
  }
}
