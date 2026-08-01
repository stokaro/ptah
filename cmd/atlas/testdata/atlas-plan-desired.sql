-- desired state for schema-plan-file scenario
CREATE TABLE `users` (
  `id` integer NOT NULL PRIMARY KEY AUTOINCREMENT,
  `name` text NOT NULL,
  `email` text NULL
);
CREATE TABLE `posts` (
  `id` integer NOT NULL PRIMARY KEY AUTOINCREMENT,
  `user_id` integer NOT NULL,
  `title` text NOT NULL,
  CONSTRAINT `fk_posts_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)
);
CREATE INDEX `idx_posts_user_id` ON `posts` (`user_id`);
