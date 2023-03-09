class read_from_postgres:

    def readFromPostgresDB(self, spark, table):
        return spark.read.format("jdbc")\
        .option("url", "jdbc:postgresql://localhost:5432/retail_db")\
        .option("dbtable", "{}".format(table)) \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .load()