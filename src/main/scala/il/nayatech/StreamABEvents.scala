package il.nayatech

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.{IntegerType, StringType, StructType, TimestampType}

object StreamABEvents {

  def main(args: Array[String]): Unit = {
    val bootstrapServers = "localhost:9092"
    val subscribeType = "subscribe"
    val topics = "Cads"

    val conf = new SparkConf().setMaster("local[2]")

    val spark = SparkSession
      .builder
      .appName("StreamABEvents")
      .config(conf)
      .getOrCreate()

    // Create dataframe representing the stream of input json from kafka
    val dfKafkaRaw = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topics)
      .load()
      .selectExpr("CAST(value AS STRING)", "CAST(timestamp as STRING)")

    dfKafkaRaw.printSchema()

    // This import is needed to use the $-notation
    import spark.implicits._


    // Convert to JSON and then parse json value
    val jsonSchema = new StructType()
      .add("EventName", StringType)
      .add("EventTime", StringType)
      .add("Unit", IntegerType)
      .add("Value", IntegerType)

    var dfJson = dfKafkaRaw.select(
      from_json($"value",jsonSchema).alias("parsed_json_values"),
      $"timestamp".alias("KafkaTimestamp").cast(TimestampType)
    )

    dfJson = dfJson.select(
      $"parsed_json_values.*",
      $"KafkaTimestamp"
    )

    dfJson.printSchema()


    // Start running the query that prints the running computed data to the console
    val query = dfJson.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", false)
      .start()

    query.awaitTermination()
  }
}
