-- change engine
set hive.execution.engine = mr;

-- find number of unique tags
SELECT COUNT (DISTINCT tag)
FROM movies.gl_tags;

-- find number of movies released in each year
SELECT startyear, COUNT(tconst)
FROM movies.imdb_title_basics
GROUP BY startyear
HAVING startyear <= 2016 AND startyear IS NOT NULL
ORDER BY startyear;

-- find variance of ratings of a user
SET uid = 1535;
SELECT VAR_SAMP(rating)
FROM movies.gl_ratings
WHERE userid = ${hiveconf:uid};

-- find average ratings of users in each year
SELECT year, avg(rating)
FROM
movies.gl_ratings
GROUP BY year
ORDER BY year;

-- find most popular words in movie titles
SELECT title, COUNT(*)
FROM
(SELECT title
FROM movies.imdb_title_basics lateral view explode(split(primarytitle,' ')) primarytitle AS title) as temp
GROUP BY title;

SELECT word, count(*) as c
FROM
(SELECT explode(words) as word
FROM
(SELECT explode(sentences(lower(primarytitle))) as words
from movies.imdb_title_basics
WHERE titletype = 'movie') AS T1) AS T2
GROUP BY word
ORDER BY c desc
LIMIT 20;