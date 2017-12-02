set hive.execution.engine=mr;
CREATE DATABASE IF NOT EXISTS movies;

CREATE EXTERNAL TABLE IF NOT EXISTS movies.imdb_name_basics_raw (
  nconst string,
  primaryName string,
  birthYear string,
  deathYear string,
  primaryProfession string,
  knownForTitles string
)
row format delimited
fields terminated by '\t'
stored as textfile
location '/hadoop2/imdb_data/name.basics/'
tblproperties("skip.header.line.count"="1"); 

CREATE TABLE IF NOT EXISTS movies.imdb_name_basics (
  nconst string,
  primaryName string,
  birthYear string,
  deathYear string,
  primaryProfession string,
  knownForTitles string
)
stored as parquet;
INSERT OVERWRITE TABLE movies.imdb_name_basics SELECT * FROM movies.imdb_name_basics_raw;



CREATE EXTERNAL TABLE IF NOT EXISTS movies.imdb_title_basics_raw (
  tconst string,
  titleType string,
  primaryTitle string,
  originalTitle string,
  isAdult string,
  startYear string,
  endYear string,
  runtimeMinutes int,
  genres string
)
row format delimited
fields terminated by '\t'
stored as textfile
location '/hadoop2/imdb_data/title.basics/'
tblproperties("skip.header.line.count"="1"); 

CREATE TABLE IF NOT EXISTS movies.imdb_title_basics (
  tconst string,
  titleType string,
  primaryTitle string,
  originalTitle string,
  isAdult string,
  startYear string,
  endYear string,
  runtimeMinutes int,
  genres string
)
stored as parquet;
INSERT OVERWRITE TABLE movies.imdb_title_basics SELECT * FROM movies.imdb_title_basics_raw;




CREATE EXTERNAL TABLE IF NOT EXISTS movies.imdb_title_principles_raw (
  tconst string,
  principalCast string
)
row format delimited
fields terminated by '\t'
stored as textfile
location '/hadoop2/imdb_data/title.principles/'
tblproperties("skip.header.line.count"="1"); 

CREATE TABLE IF NOT EXISTS movies.imdb_title_principles (
  tconst string,
  principalCast string
)
stored as parquet;
INSERT OVERWRITE TABLE movies.imdb_title_principles SELECT * FROM movies.imdb_title_principles_raw;
