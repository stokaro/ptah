plan "20260801102801" {
  from      = "2Avyplv6jw8kAsH/g2YFPkfnp+UNBpomMXPUl/4R4+Q="
  to        = "YEugbm2aJqmXFA8dDrzmqLPC4tiNUrXe6YCrvazKOiY="
  migration = <<-SQL
  -- Add column "email" to table: "users"
  ALTER TABLE `users` ADD COLUMN `email` text NULL;
  -- Create "posts" table
  CREATE TABLE `posts` (
    `id` integer NOT NULL PRIMARY KEY AUTOINCREMENT,
    `user_id` integer NOT NULL,
    `title` text NOT NULL,
    CONSTRAINT `fk_posts_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON UPDATE NO ACTION ON DELETE NO ACTION
  );
  -- Create index "idx_posts_user_id" to table: "posts"
  CREATE INDEX `idx_posts_user_id` ON `posts` (`user_id`);
  SQL
}
