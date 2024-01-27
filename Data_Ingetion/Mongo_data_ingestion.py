mongoURI = 'mongodb://dev:xyz@x.x.x.x:yyyy/'
mongoDatabaseName = 'DB_name'

initialDF = \
    spark \
    .read \
    .format("com.mongodb.spark.sql.DefaultSource") \
    .option("uri", mongoURI) \
    .option("database", mongoDatabaseName) \
    .option("collection", "orders") \
    .option("sampleSize", 50000) \
    .load()
