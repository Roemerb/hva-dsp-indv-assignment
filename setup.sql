CREATE DATABASE IF NOT EXISTS hva_bigdata_indv_assignment;

USE hva_bigdata_indv_assignment;

CREATE TABLE IF NOT EXISTS `movielens_tmdb_imdb` (
	`id` INT PRIMARY KEY NOT NULL,
	`tmdb_id` INT NOT NULL,
	`imdb_id` INT NOT NULL
);

CREATE TABLE IF NOT EXISTS `tmdb_genres` (
	`id` INT PRIMARY KEY NOT NULL,
	`name` VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS `tmdb_reviews` (
	`id` INT PRIMARY KEY AUTO_INCREMENT NOT NULL,
	``
);

CREATE TABLE IF NOT EXISTS `tmdb_movies`(
	`id` INT AUTO_INCREMENT PRIMARY KEY NOT NULL,
	`tmdb_id` INT NOT NULL,
	`title` VARCHAR(255) NOT NULL,
	`release_date` DATE,
	`revenue` INT,
	`budget` INT,
	`runtime` INT,
	`description` TEXT,
	`vote_avg` FLOAT,
	`vote_cnt` INT,
	`status` VARCHAR(100),
	`is_adult` BOOL,
	`backdrop_uri` TEXT,
	`orig_lang` VARCHAR(5),
	`pop` FLOAT,
	`production_company` VARCHAR(255)
);