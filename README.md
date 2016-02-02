Spark demo for Cassandra Days Germany Feb 2016
==============================================

This repo contains the project of the talk
_You’ve got the lighter, let’s start a fire: Spark and Cassandra_
held on the _Cassandra Days Germany_ on Feb 9th+11th in Munich and Berlin.

# Requirements

The demo requires DataStax Enterprise 4.8.2 or newer.
However, with some tweaks it could work with OSS Apache Cassandra + Spark.

Additionally you will need at least a recent release of Apache Kafka, Java 7 (for DSE) and Scala 2.10.

# Setup instructions

1. Install DataStax Enterprise 4.8.2 or newer
1. Start DSE with Spark using `bin/dse -k` from the DSE distribution (just `dse -k`, if you use a non-tarball DSE distribution)
1. Setup the Cassandra database schema using `cqlsh -f create-schema.cql`
1. Open the `start-kafka.sh` file and change the value of `KAFKA_DIR` variable to the home directory of your kafka installation

# Run instructions

Hints:
* If you want to modify and play with this demo on your own,
  keep the `--daemon` flag in the gradlew command line options.
  Otherwise, feel free to remove that option.
* You will need a working internet connection both to build the
  demo and to open the web application, as it uses Google Maps + Charts.

Steps to run the demo:

1. Start DSE with Spark (if not already running) using `bin/dse -k` from the DSE distribution (just `dse -k`, if you use a non-tarball DSE distribution)
1. Open a new terminal window and start Kafka (don't forget ZK)
   `./start-kafka.sh`
   and wait until it has finished. If you run file this multiple times,
   it will complain that the topic already exists - that's ok.
1. Build the demo using `./gradlew --daemon jar shadowJar`
1. Open a new terminal window and start the Spark Streaming job
   `$DSE_HOME/bin/dse spark-submit cstar-spark-stream/build/libs/cstar-spark-stream-all.jar`
1. Open a new terminal window and start the web application
   `./gradlew --daemon :cstar-spark-webapp:run`
1. Point your browser to the web application root URL
   `http://localhost:8080/`
1. Open a new terminal window and start the data generator
   `scala cstar-spark-generate/build/libs/cstar-spark-generate-all.jar`
