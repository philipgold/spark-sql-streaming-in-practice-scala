package il.nayatech

import java.util.Properties
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}

object ProducerKafkaFlightData {
  def main(args: Array[String]): Unit = {


    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    val conf = new SparkConf().setMaster("local[2]")

    val spark = SparkSession
      .builder
      .appName("ProducerKafkaFlightData")
      .config(conf)
      .getOrCreate()

    val df_main = spark
      .read
      .format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("data/flight-history")

    df_main.printSchema()
    df_main.show()

    import scala.collection.JavaConversions._
    val jsonList = df_main.toJSON.collectAsList()

    //TODO: Add sorting by timestamp for the json list

    for (jsonObj <- jsonList){
      println(jsonObj)
      val data = new ProducerRecord[String, String]("Cads", jsonObj)
      producer.send(data)
    }

    spark.stop()
  }
}