env "local" {
  url = getenv("PTAH_ATLAS_PROJECT_CONFIG_E2E_URL")
  migration {
    dir              = "file://migrations"
    revisions_schema = "ptah_issue_276"
    exec_order       = "linear"
  }
}
