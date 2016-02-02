/*
 * Cassandra-Spark Demo - Cassandra Days Germany 2016
 * (C) 2016 DataStax
 */
package com.datastax.cstar_spark_demo

import java.io.{ObjectInputStream, ByteArrayInputStream}
import java.util.Date

import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.streaming._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

// (duplicated from GenerateData for demonstration purposes)
case class Power(
                  id: Long,
                  name: String,
                  lat: Float,
                  lon: Float,
                  population: Int,
                  area: Float,
                  powerConsumptionDiff: Float
                )

case class TimestampedPower(timestamp: Date,
                            power: Power)

object StreamFromKafka
{

  val kafkaParams: Map[String, String] = Map("zookeeper.connect" -> "127.0.0.1:2181",
                                             "group.id" -> "spark-stream-group")
  val topics: Map[String, Int] = Map("cstarSpark" -> 1)

  val cassandraKeyspace = "cstar_day_spark"

  val windowDuration: Duration = Seconds(15)
  val slideDuration: Duration = Seconds(1)

  def main(args: Array[String])
  {
    val conf = new SparkConf(true)
                // We don't need to configure the Spark Master and other stuff with DSE
                // 'dse spark-submit' does that for us!
                .setAppName("cstarSparkStreaming")
                .set("spark.cleaner.ttl", "3600")

    val ssc = new StreamingContext(conf, slideDuration)

    // Setup the Kafka topic Spark DStream
    val records: ReceiverInputDStream[(String, Array[Byte])] =
      KafkaUtils.createStream[String, Array[Byte], StringDecoder, DefaultDecoder](
        ssc,
        kafkaParams,
        topics,
        StorageLevel.MEMORY_AND_DISK_SER_2)

    val powerdiff = records
      // Deserialize the power record from the Kafka topic.
      .map(keyValue => deserialize[Power](keyValue._2))
      // Extract the ID of the city
      .map(power => (power.id, power))
      // Reduce by ID of the city (aggregate the power consumption by city and time window)
      .reduceByKeyAndWindow((power1: Power, power2: Power) =>
                              power1.copy(powerConsumptionDiff =
                                            power1.powerConsumptionDiff +
                                            power2.powerConsumptionDiff),
                            windowDuration,
                            slideDuration)
      // Inject the current time-window timestamp. We need the timestamp of the
      // current window a naive "current timestamp" would return _some_ timestamp
      // of _some_ node.
      .transform((rdd, time) => rdd.map(power =>
                                        {
                                          // inject the timestamp of the current
                                          // window to the power data
                                          TimestampedPower(new Date(time.milliseconds),
                                                           power._2)
                                        }))
      // Map the case class into a tuple to be stored in Cassandra
      .map(timestampedPower => (timestampedPower.timestamp, timestampedPower.power.id,
                                timestampedPower.power.lat, timestampedPower.power.lon,
                                timestampedPower.power.name, timestampedPower.power.area,
                                timestampedPower.power.population,
                                timestampedPower.power.powerConsumptionDiff))
      // Cache the result since it's needed multiple times below
      .cache

    // Save records per time-window
    //
    // That's the power consumption over the last 60 seconds (windowDuration)
    // for each city by time
    //
    // CREATE TABLE power_by_time ( ... PRIMARY KEY ( timewindow, city_id ) )
    // --> timewindow is the partition key
    // --> city_id is the clustering key
    powerdiff.saveToCassandra(cassandraKeyspace, "power_by_time",
                              SomeColumns("timewindow", "city_id", "lat", "lon",
                                          "name", "area", "population", "power"))

    // Save records per city
    //
    // That's the power consumption over the last 60 seconds (windowDuration)
    // for time-window by city
    //
    // CREATE TABLE power_by_city ( ... PRIMARY KEY ( city_id, timewindow ) )
    // --> city_id is the partition key
    // --> timewindow is the clustering key
    powerdiff.saveToCassandra(cassandraKeyspace, "power_by_city",
                              SomeColumns("timewindow", "city_id", "lat", "lon",
                                          "name", "area", "population", "power"))

    // Save last processed timestamp
    //
    // For this demo we have to save the current (means: "last") timestamp by which we can
    // find the current power consumption per city in the table "powerdiff"
    powerdiff.map(tup => tup._1)
      .reduce((t, x) => t)
      // at this point we only have the one "row" containing the
      // timestamp to be saved to Cassandra
      .map(t => ("dummy", t))
      .saveToCassandra(cassandraKeyspace, "lastwindow",
                       SomeColumns("dummy", "timewindow"))

    ssc.start()

    ssc.awaitTermination()
  }

  def deserialize[T](input: Array[Byte]): T =
  {
    val in = new ByteArrayInputStream(input)
    val oin = new ObjectInputStream(in)
    oin.readObject().asInstanceOf[T]
  }
}
