/*
 * Cassandra-Spark Demo - Cassandra Days Germany 2016
 * (C) 2016 DataStax
 */
package com.datastax.cstar_spark_demo

import java.io.{ObjectOutputStream, ByteArrayOutputStream}
import java.util.Properties

import com.datastax.driver.core.{Session, Cluster}
import com.datastax.spark.connector.util.JavaApiHelper
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

// Master data: all cities.
case class City(
                 id: Long,
                 name: String,
                 lat: Float,
                 lon: Float,
                 population: Int,
                 area: Float
               ) extends scala.Serializable

// Power sensor measurements.
// Combined with city data here. Better approach would
// be to "use" the city data just from the web UI.
case class Power(
                  id: Long,
                  name: String,
                  lat: Float,
                  lon: Float,
                  population: Int,
                  area: Float,
                  powerConsumptionDiff: Float
                ) extends scala.Serializable

object GenerateData
{

  val cassandraContactPoint = "127.0.0.1"
  val cassandraClusterName = "Test Cluster"
  val cassandraKeyspace = "cstar_day_spark"

  private def connectToCassandra: (Cluster, Session) =
  {
    val cluster = Cluster.builder()
      .addContactPoint(cassandraContactPoint)
      .withClusterName(cassandraClusterName)
      .build()
    val session = cluster.connect(cassandraKeyspace)
    (cluster, session)
  }

  private def setupKafkaProducer: Producer[String, Array[Byte]] =
  {
    val props = new Properties()
    props.put("metadata.broker.list", "127.0.0.1:9092")
    props.put("serializer.class", "kafka.serializer.DefaultEncoder")
    props.put("key.serializer.class", "kafka.serializer.StringEncoder")
    // "sync" just to not overwhelm the consumer ;)
    props.put("producer.type", "sync")
    val config = new ProducerConfig(props)
    val producer = new Producer[String, Array[Byte]](config)
    producer
  }

  def main(args: Array[String])
  {
    val (cluster: Cluster, session: Session) = connectToCassandra

    try
    {
      val producer = setupKafkaProducer

      try
      {
        // Fetch all cities from our cities table and hold them locally.
        // (this is just master data - and not a good data model, but it works for this demo)
        val cities = JavaApiHelper.toScalaSeq(
          session.execute("SELECT loc_id, name, lat, lon, population, area FROM cities"))
                 .map(r => City(r.getLong("loc_id"),
                                r.getString("name"),
                                r.getFloat("lat"),
                                r.getFloat("lon"),
                                r.getInt("population"),
                                r.getFloat("area")))

        // Get the total population (required to return random cities.
        // Cities with higher population are returned more often).
        val totalPopulation = cities.map(_.population).sum

        while (true)
        {

            // get a random city (weighted by population)
            val city = randomCity(cities, totalPopulation)

            // randomly choose the power consumption
            val powerConsumptionDiff = scala.util.Random.nextFloat() * 100

            // Generate a data record that contains the new, additional
            // power consumption in a city. That object is sent via Kafka
            // and received in the Spark Streaming module.
            val power = Power(city.id, city.name, city.lat, city.lon,
                              city.population, city.area,
                              powerConsumptionDiff)

            // send the serialized Power object via Kafka
            producer.send(new KeyedMessage("cstarSpark", serialize(power)))

            // Just wait and sleep (don't let my laptop burn!)
            Thread.sleep(100)

        }
      }
      finally
      {
        producer.close()
      }
    }
    finally
    {
      cluster.close()
    }
  }

  def randomCity(cities: Seq[City], totalPopularion: Integer) =
  {
    var p = totalPopularion * scala.util.Random.nextDouble()
    // weighted by city population (not the best implementation, but works for this demo)
    cities.find(city =>
                {
                  p -= city.population
                  p <= 0
                }).get
  }

  def serialize(obj: Serializable): Array[Byte] =
  {
    val out = new ByteArrayOutputStream()
    val oout = new ObjectOutputStream(out)
    oout.writeObject(obj)
    oout.close()
    out.toByteArray
  }
}
