/*
 * Cassandra-Spark Demo - Cassandra Days Germany 2016
 * (C) 2016 DataStax
 */
package com.datastax.cstar_spark_demo

import akka.actor.Actor
import spray.routing._
import spray.http._
import spray.httpx.SprayJsonSupport._
import MasterJsonProtocol._
// take care, the last two imports are not unused (your IDE is probably wrong ;) )

class KillrPowrServiceActor extends Actor with KillrPowrService
{

  // the HttpService trait defines only one abstract member, which
  // connects the services environment to the enclosing actor or test
  def actorRefFactory = context

  // this actor only runs our route, but you could add
  // other things here, like request stream processing
  // or timeout handling
  def receive = runRoute(route)
}


// this trait defines our service behavior independently from the service actor
trait KillrPowrService extends HttpService
{

  val route =
  // API methods
    pathPrefix("api")
    {
      //
      // GET /api/most-recent-timestamp
      //
      // API method to return the value in CQL table "lastwindow" -
      // i.e. the last timestamp that has been processed
      path("most-recent-timestamp")
      {
        get
        {
          complete(CassandraDAO.fetchMostRecentTimestamp)
        }
      } ~
      //
      // GET /api/by-timestamp/<TIMESTAMP>
      //
      // API method to return the cities and power consumptions for a
      // specific time window from CQL table "power_by_time"
      path("by-timestamp" / LongNumber)
      { timestamp =>
        get
        {
          complete(CassandraDAO.fetchByTimestamp(timestamp))
        }
      } ~
      //
      // GET /api/by-city/<CITY_ID>?limit=<LIMIT>
      //
      // API method to return the power consumptions by time for a
      // specific city from CQL table "power_by_city"
      path("by-city" / LongNumber)
      { cityId =>
        get
        {
          parameters('limit.as[Int] ?)
          {
            limit => complete(CassandraDAO.fetchByCity(cityId, limit.getOrElse(60)))
          }
        }
      } ~
      //
      // /api/recent
      //
      // API method to return the cities and power consumptions
      // for the most recent time window from CQL table "power_by_time"
      path("recent")
      {
        get
        {
          complete(CassandraDAO.fetchMostRecent())
        }
      }
    } ~
    //
    // The home page and other static content
    compressResponse()(getFromResourceDirectory("static")) ~
    //
    // Redirect to default index.html
    path("")
    {
      redirect("index.html", StatusCodes.MovedPermanently)
    }
}
