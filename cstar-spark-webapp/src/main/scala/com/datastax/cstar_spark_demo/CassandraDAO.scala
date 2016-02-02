/*
 * Cassandra-Spark Demo - Cassandra Days Germany 2016
 * (C) 2016 DataStax
 */
package com.datastax.cstar_spark_demo

import java.util.Date

import com.datastax.driver.core.{Row, Session, Cluster}
import com.datastax.spark.connector.util.JavaApiHelper

object CassandraDAO
{
  val cassandraContactPoint = "127.0.0.1"
  val cassandraClusterName = "Test Cluster"
  val cassandraKeyspace = "cstar_day_spark"

  val (cluster: Cluster, session: Session) = connectToCassandra // TODO you should really close cluster+session on application shutdown

  val pstmtMostRecentTimestamp =
    session.prepare("SELECT timewindow FROM lastwindow")
  val pstmtPowerByTimestamp =
    session.prepare("SELECT city_id, lat, lon, name, area, population, power " +
                    "FROM power_by_time " +
                    "WHERE timewindow = ?")
  val pstmtPowerByCity =
    session.prepare("SELECT timewindow, city_id, lat, lon, name, area, population, power " +
                    "FROM power_by_city " +
                    "WHERE city_id = ? LIMIT ?")

  /**
    * Retrieve the timestamp of the most recent power-consumption records.
    */
  def fetchMostRecentTimestamp: MostRecentTimestamp =
  {
    val resultSet = session.execute(pstmtMostRecentTimestamp.bind)
    val lastTimestamp = if (resultSet.isExhausted) 0 else resultSet.one().getDate(0).getTime
    MostRecentTimestamp(lastTimestamp)
  }

  /**
    * Fetch power consumption by given timestamp.
    */
  def fetchByTimestamp(timestamp: Long): PowerByTimestamp =
  {
    val resultSet = session.execute(pstmtPowerByTimestamp.bind
                                      .setDate(0, new Date(timestamp)))
    val powerRecords = JavaApiHelper.toScalaSeq(resultSet)
      .map(row => PowerAndCity(
        cityFromRow(row),
        row.getFloat("power")
      ))

    PowerByTimestamp(timestamp, powerRecords)
  }

  /**
    * Fetch power consumption for all cities for most recent data.
    */
  def fetchMostRecent(): PowerByTimestamp =
  {
    // that's bad - we have two reads from C*
    fetchByTimestamp(fetchMostRecentTimestamp.timestamp)
  }

  /**
    * Fetch power consumption history for a city.
    */
  def fetchByCity(cityId: Long, limit: Int): PowerByCity =
  {
    val resultSet = session.execute(pstmtPowerByCity.bind
                                      .setLong(0, cityId)
                                      .setInt(1, limit))
    val powerRows = JavaApiHelper.toScalaSeq(resultSet)

    var city: Option[City] = None

    val powerRecords = powerRows
      .map(row =>
           {
             if (city.isEmpty) city = Some(cityFromRow(row))
             TimestampAndPower(row.getDate("timewindow").getTime, row.getFloat("power"))
           })

    PowerByCity(city, powerRecords)
  }

  def cityFromRow(row: Row): City =
  {
    City(
      row.getLong("city_id"),
      row.getString("name"),
      row.getFloat("lat"),
      row.getFloat("lon"),
      row.getInt("population"),
      row.getFloat("area"))
  }

  private def connectToCassandra: (Cluster, Session) =
  {
    val cluster = Cluster.builder()
      .addContactPoint(cassandraContactPoint)
      .withClusterName(cassandraClusterName)
      .build()
    val session = cluster.connect(cassandraKeyspace)
    (cluster, session)
  }

}
