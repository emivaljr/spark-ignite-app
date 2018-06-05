package org.test

import org.apache.spark.sql.SparkSession
import java.lang.{Long ⇒ JLong, String ⇒ JString}
import org.apache.ignite.cache.query.SqlFieldsQuery
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.{Ignite, Ignition}
import org.apache.ignite.spark.IgniteDataFrameSettings._

object SimpleApp {
  private val CONFIG = "example-ignite.xml"

  /**
    * Test cache name.
    */
  private val CACHE_NAME = "testCache"


  def main(args: Array[String]) {
    setupServerAndData

    //val logFile = "/home/emival/Dev/git/ignite/README.md" // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application").master("local[*]").getOrCreate()
/*
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
*/
    val cfgPath: String = "example-ignite.xml"

    val df = spark.read
      .format(FORMAT_IGNITE)               // Data source type.
      .option(OPTION_TABLE, "person")      // Table to read.
      .option(OPTION_CONFIG_FILE, cfgPath) // Ignite config.
      .load()

    df.createOrReplaceTempView("person")

    val igniteDF = spark.sql("SELECT * FROM person WHERE name = 'Mary Major'")
    igniteDF.show()
    //println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }

  def setupServerAndData: Ignite = {
    //Starting Ignite.
    val ignite = Ignition.start(CONFIG)

    //Creating first test cache.
    val ccfg = new CacheConfiguration[JLong, JString](CACHE_NAME).setSqlSchema("PUBLIC")

    val cache = ignite.getOrCreateCache(ccfg)

    //Creating SQL tables.
    cache.query(new SqlFieldsQuery(
      "CREATE TABLE city (id LONG PRIMARY KEY, name VARCHAR) WITH \"template=replicated\"")).getAll

    cache.query(new SqlFieldsQuery(
      "CREATE TABLE person (id LONG, name VARCHAR, city_id LONG, PRIMARY KEY (id, city_id)) " +
        "WITH \"backups=1, affinityKey=city_id\"")).getAll

    cache.query(new SqlFieldsQuery("CREATE INDEX on Person (city_id)")).getAll

    //Inserting some data to tables.
    var qry = new SqlFieldsQuery("INSERT INTO city (id, name) VALUES (?, ?)")

    cache.query(qry.setArgs(1L.asInstanceOf[JLong], "Forest Hill")).getAll
    cache.query(qry.setArgs(2L.asInstanceOf[JLong], "Denver")).getAll
    cache.query(qry.setArgs(3L.asInstanceOf[JLong], "St. Petersburg")).getAll

    qry = new SqlFieldsQuery("INSERT INTO person (id, name, city_id) values (?, ?, ?)")

    cache.query(qry.setArgs(1L.asInstanceOf[JLong], "John Doe", 3L.asInstanceOf[JLong])).getAll
    cache.query(qry.setArgs(2L.asInstanceOf[JLong], "Jane Roe", 2L.asInstanceOf[JLong])).getAll
    cache.query(qry.setArgs(3L.asInstanceOf[JLong], "Mary Major", 1L.asInstanceOf[JLong])).getAll
    cache.query(qry.setArgs(4L.asInstanceOf[JLong], "Richard Miles", 2L.asInstanceOf[JLong])).getAll

    ignite
  }
}