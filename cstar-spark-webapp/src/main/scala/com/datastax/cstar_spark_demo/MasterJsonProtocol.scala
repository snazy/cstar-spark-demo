/*
 * Cassandra-Spark Demo - Cassandra Days Germany 2016
 * (C) 2016 DataStax
 */
package com.datastax.cstar_spark_demo

import spray.json.DefaultJsonProtocol

case class MostRecentTimestamp(timestamp: Long)

case class City(
                 cityId: Long,
                 name: String,
                 lat: Float,
                 lon: Float,
                 population: Int,
                 area: Float
               )

case class PowerAndCity (
                        city: City,
                        power: Float
                        )

case class PowerByTimestamp(timestamp: Long, power: Seq[PowerAndCity])

case class TimestampAndPower(timestamp: Long, power: Float)

case class PowerByCity(city: Option[City], power: Seq[TimestampAndPower])

object MasterJsonProtocol extends DefaultJsonProtocol {

  implicit val mostRecentTimestampFormat = jsonFormat1(MostRecentTimestamp)

  implicit val cityFormat = jsonFormat6(City)

  implicit val timestampAndPowerFormat = jsonFormat2(TimestampAndPower)

  implicit val powerAndCityFormat = jsonFormat2(PowerAndCity)

  implicit val powerByCityFormat = jsonFormat2(PowerByCity)

  implicit val powerByTimestampFormat = jsonFormat2(PowerByTimestamp)
}
