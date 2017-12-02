CREATE DATABASE IF NOT EXISTS movies;

CREATE EXTERNAL TABLE IF NOT EXISTS movies.gl_movies_raw (
  movieId bigint,
  title string,
  genres string
)
row format delimited
fields terminated by ','
stored as textfile
location '/hadoop2/grouplens_data/movies/'
tblproperties("skip.header.line.count"="1"); 

CREATE TABLE IF NOT EXISTS movies.gl_movies (
  movieId bigint,
  title string,
  genres string
)
stored as parquet;
INSERT OVERWRITE TABLE movies.gl_movies SELECT * FROM movies.gl_movies_raw;




CREATE EXTERNAL TABLE IF NOT EXISTS movies.gl_ratings_raw (
  userId bigint,
  movieId bigint,
  rating double,
  t bigint 
)
row format delimited
fields terminated by ','
stored as textfile
location '/hadoop2/grouplens_data/ratings/'
tblproperties("skip.header.line.count"="1"); 

CREATE TABLE IF NOT EXISTS movies.gl_ratings (
  userId bigint,
  movieId bigint,
  rating double,
  year string 
)
stored AS parquet;
INSERT OVERWRITE TABLE movies.gl_ratings
SELECT userId, movieId, rating, from_unixtime(t, 'YYYY') AS year
FROM movies.gl_ratings_raw;




CREATE EXTERNAL TABLE IF NOT EXISTS movies.gl_links_raw (
  movieId bigint,
  imdbId bigint,
  tmbdId bigint 
)
row format delimited
fields terminated by ','
stored as textfile
location '/hadoop2/grouplens_data/links/';
tblproperties("skip.header.line.count"="1"); 

CREATE TABLE IF NOT EXISTS movies.gl_links (
  movieId bigint,
  imdbId bigint,
  tmbdId bigint 
)
stored as parquet;
INSERT OVERWRITE TABLE movies.gl_links SELECT * FROM movies.gl_links_raw;





CREATE EXTERNAL TABLE IF NOT EXISTS movies.gl_tags_raw (
  userId bigint,
  movieId bigint,
  tag string,
  t timestamp  
)
row format delimited
fields terminated by ','
stored as textfile
location '/hadoop2/grouplens_data/tags/';
tblproperties("skip.header.line.count"="1"); 

CREATE TABLE IF NOT EXISTS movies.gl_tags (
  userId bigint,
  movieId bigint,
  tag string,
  t timestamp  
)
stored as parquet;
INSERT OVERWRITE TABLE movies.gl_tags SELECT * FROM movies.gl_tags_raw;
