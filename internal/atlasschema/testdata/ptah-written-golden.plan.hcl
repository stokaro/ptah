plan "plan_6d78a9ec5390" {
  from      = "sha256:2ef81def17f625ec4fc7927e136e516022e244ab587bb702b5b71d38b05cbe27"
  to        = "sha256:c4fc6302f3cc08997acbb8b8d6ae52eabcbd9c6604a9835305035b1522e03b23"
  migration = <<-SQL
  CREATE TABLE "posts" (
    "id" integer PRIMARY KEY AUTOINCREMENT,
    "user_id" integer NOT NULL,
    "title" TEXT NOT NULL,
    CONSTRAINT "fk_posts_user" FOREIGN KEY ("user_id") REFERENCES "users" ("id")
  );
  ALTER TABLE "users" ADD COLUMN "email" TEXT;
  CREATE INDEX IF NOT EXISTS "idx_posts_user_id" ON "posts" ("user_id");
  SQL
}
