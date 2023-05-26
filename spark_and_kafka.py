import pyspark 
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, LongType, StringType, DateType

spark = SparkSession \
    .builder \
    .appName("Traitement des données en streaming") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/log_analysis.spark_data") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/log_analysis.spark_data") \
    .config("spark.jars", "mongo-spark-connector_2.12-10.1.1.jar") \
    .getOrCreate()

# spark.conf.set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")

df_stream = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092")\
            .option("subscribe", "aggregating_log_servers").load()

# Je caste la valeur de sortie du dataframe en chaîne de caractères 
df_stream = df_stream.selectExpr("CAST(value AS STRING)")

# Affichage de la sortie contenant toutes les colonnes à chaque 5 secondes
df_stream.writeStream.trigger(processingTime='5 seconds').outputMode("append").option("truncate", "false").format("console").start()

# Sélectionner les colonnes ayant des valeurs différentes de nulles
df_filtred = df_stream.select(get_json_object(col("value"), "$.host").alias("host"),
                              get_json_object(col("value"), "$.time").alias("time"),
                              get_json_object(col("value"), "$.method").alias("method"),
                              get_json_object(col("value"), "$.url").alias("url"), 
                              get_json_object(col("value"), "$.response").alias("response"),
                              get_json_object(col("value"), "$.bytes").alias("bytes"))

df_filtred.printSchema()

# Je vais changer les types de données des colonnes
df_selected_schema = df_filtred.withColumn("host", col("host").cast(StringType())) \
                                .withColumn("time", col("time").cast(LongType())) \
                                .withColumn("method", col("method").cast(StringType())) \
                                .withColumn("url", col("url").cast(StringType())) \
                                .withColumn("response", col("response").cast(IntegerType())) \
                                .withColumn("bytes", col("bytes").cast(IntegerType()))
# Affichage du schéma du nouveau dataframe
df_selected_schema.printSchema()

# Conversion de la colonne time en datetime et  génération de 2 colonnes qui vont séparer la date au temps
df_selected_schema = df_selected_schema.withColumn("datetime", from_unixtime(col('time'))) \
                                        .withColumn("date", split(col("datetime"), " ")[0]) \
                                        .withColumn("times", split(col('datetime'), " ")[1])
# df_convert = df_selected_schema.select(from_unixtime(col('time')).alias("datetime"))
# df_convert.printSchema()

# Maintenant que j'ai généré 2 nouvelles colonnes sur la colonne datetime, je vais la supprimer avec la colonne time.
df_selected_schema = df_selected_schema.drop('time')
df_selected_schema = df_selected_schema.drop('datetime')

# df_stream3 =  df_selected_schema.writeStream.format("mongodb").option("checkpointLocation", "/tmp/pyspark/").option("uri", "mongodb://127.0.0.1/log_analysis.spark_data").outputMode("append")
# df_stream3 = (df_selected_schema.writeStream.format("mongodb")
#   .option("spark.mongodb.connection.uri", "monngodb://localhost/")
#   .option('spark.mongodb.database', "log_analysis")
#   .option('spark.mongodb.collection', "spark_data"))
# query = df_stream3.start()

# Lancement du flux de message du nouveau dataframe dans la console toutes les 5 secondes
df_stream2 = df_selected_schema.writeStream.trigger(processingTime='5 seconds').outputMode("append").option("truncate", "false").format("console").start()


# # Attendre que le flux se termine
# df_stream3.awaitTermination()
# df_stream2.awaitTermination()



