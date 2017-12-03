# initialize hive_context
from pyspark.sql import HiveContext
hive_context = HiveContext(sc)

# join user rating table with imdb table to obtain genres of each user rating
user_profiles = hive_context.sql("""
SELECT ratings.rating, ratings.userid, imdb.genres
FROM movies.gl_links AS links
JOIN movies.gl_ratings AS ratings ON links.movieid = ratings.movieid
JOIN movies.imdb_title_basics AS imdb ON CAST(substr(imdb.tconst, 3) as INT) = links.imdbid
""").persist()
user_profiles.show()

# +------+------+------------------+
# |rating|userid|            genres|
# +------+------+------------------+
# |   5.0|   741|  Mystery,Thriller|
# |   4.0|  1227|  Mystery,Thriller|
# |   2.5|  1741|  Mystery,Thriller|
# |   4.0|  1972|  Mystery,Thriller|
# |   3.0|  2171|  Mystery,Thriller|
# |   4.0|  4222|  Mystery,Thriller|
# |   5.0|  4358|  Mystery,Thriller|
# |   4.0|  5354|  Mystery,Thriller|
# |   3.0|  5832|  Mystery,Thriller|
# |   1.0|   347|Comedy,Crime,Drama|
# |   2.5|   413|Comedy,Crime,Drama|
# |   3.5|  1763|Comedy,Crime,Drama|
# |   3.5|  2033|Comedy,Crime,Drama|
# |   4.0|  2113|Comedy,Crime,Drama|
# |   2.0|  3201|Comedy,Crime,Drama|
# |   4.5|  3284|Comedy,Crime,Drama|
# |   4.0|  3529|Comedy,Crime,Drama|
# |   4.0|  3774|Comedy,Crime,Drama|
# |   4.0|  4222|Comedy,Crime,Drama|
# |   3.5|  4302|Comedy,Crime,Drama|
# +------+------+------------------+
# only showing top 20 rows

user_profiles.registerTempTable("user_profiles")

# split each rating into individual ratings for each genre
user_profiles_genre = hive_context.sql("""
SELECT userid, rating, explode(split(genres, ',')) as genre
FROM user_profiles
""")
user_profiles_genre.registerTempTable("user_profiles_genre")
user_profiles_genre.show(5)

# +------+------+--------+
# |userid|rating|   genre|
# +------+------+--------+
# |   741|   5.0| Mystery|
# |   741|   5.0|Thriller|
# |  1227|   4.0| Mystery|
# |  1227|   4.0|Thriller|
# |  1741|   2.5| Mystery|
# +------+------+--------+

# find top 10 most frequently rated genres
top_genres = hive_context.sql("""
SELECT genre, count(*) as count
FROM user_profiles_genre
GROUP BY genre
ORDER by count DESC
LIMIT 10
""").persist()
top_genres.registerTempTable("top_genres")
top_genres.show()

# +---------+------+
# |    genre| count|
# +---------+------+
# |    Drama|494668|
# |   Comedy|367723|
# |   Action|252796|
# |Adventure|249086|
# |    Crime|192021|
# |  Romance|158370|
# | Thriller|153041|
# |   Sci-Fi|115751|
# |  Fantasy|104204|
# |  Mystery| 81931|
# +---------+------+

# filter ratings to keep rating for top 10 genres
user_profiles_genre_top_10 = hive_context.sql("""
SELECT user_profiles_genre.*
FROM user_profiles_genre
JOIN top_genres ON top_genres.genre = user_profiles_genre.genre
""").persist()
user_profiles_genre_top_10.registerTempTable("user_profiles_genre_top_10")

# calculate average ratings of genres per user
user_average_rating_per_genre = hive_context.sql("""
SELECT userid, genre, avg(rating) as rating
FROM user_profiles_genre_top_10
GROUP BY userid, genre
""").persist()
user_average_rating_per_genre.registerTempTable("user_average_rating_per_genre")
user_average_rating_per_genre.show(5)

# +------+---------+------------------+
# |userid|    genre|            rating|
# +------+---------+------------------+
# |  1568|    Drama|3.4171779141104293|
# |  4222|Adventure|3.9038461538461537|
# |  1135|   Comedy|3.2447257383966246|
# |  2005|Adventure| 3.423469387755102|
# |   471|   Comedy| 3.315934065934066|
# +------+---------+------------------+

# For each user, create an array where each value is the average rating for a specific genre
# If a user didn't rate a genre, set it to 2.5
user_features = hive_context.sql("""
SELECT userid
  ,array( MAX( if(genre = 'Drama', rating, 2.5) )
  ,MAX( if(genre = 'Comedy', rating, 2.5) )
  ,MAX( if(genre = 'Action', rating, 2.5) )
  ,MAX( if(genre = 'Adventure', rating, 2.5) )
  ,MAX( if(genre = 'Crime', rating, 2.5) )
  ,MAX( if(genre = 'Romance', rating, 2.5) )
  ,MAX( if(genre = 'Thriller', rating, 2.5) )
  ,MAX( if(genre = 'Sci-Fi', rating, 2.5) )
  ,MAX( if(genre = 'Fantasy', rating, 2.5) )
  ,MAX( if(genre = 'Mystery', rating, 2.5) )) as array_features
FROM user_average_rating_per_genre
GROUP BY userid
""").persist()
user_features.show(5)

# +------+--------------------+
# |userid|      array_features|
# +------+--------------------+
# |  5231|[3.44796380090497...|
# |   631|[3.60634328358208...|
# |  1831|[3.59459459459459...|
# |  3031|[3.01932367149758...|
# |  5831|[3.9, 3.473684210...|
# +------+--------------------+

# Conver genre avg-rating arrays into vectors
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.functions import udf
list_to_vector_udf = udf(lambda l: Vectors.dense(l), VectorUDT())
user_features_vector = user_features.withColumn("features", list_to_vector_udf(user_features["array_features"])).persist()

# Run K-Mean Algorithm to cluster users into groups
from pyspark.ml.clustering import KMeans
kmeans = KMeans().setK(5).setSeed(1)
model = kmeans.fit(user_features_vector)
model.clusterCenters()

# [array([ 3.93979085,  3.66221042,  3.60989922,  3.69382825,  3.92316072,
#          3.74491668,  3.86306208,  3.61497009,  3.6637981 ,  4.0251613 ]),
#  array([ 3.13443769,  2.89136456,  2.80996144,  2.89552402,  3.04036622,
#          2.97911897,  2.95548942,  2.8200217 ,  2.91326227,  2.93743451]),
#  array([ 3.5970148 ,  3.27293367,  3.18670232,  3.27667175,  3.57371493,
#          3.37325683,  3.48681869,  3.19204615,  3.26238953,  3.603238  ]),
#  array([ 4.25650413,  4.06026702,  4.12831383,  4.19246469,  4.24075282,
#          4.13578148,  4.23325178,  4.11196686,  4.19718261,  4.21331062]),
#  array([ 3.76557656,  3.58547697,  3.6613167 ,  3.7716317 ,  3.66067028,
#          3.64008411,  3.52101257,  3.57330586,  3.75443242,  2.95421596])]