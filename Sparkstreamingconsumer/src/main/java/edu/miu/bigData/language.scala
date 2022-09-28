
package edu.miu.bigData

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._

object language {

  def main(args: Array[String]) {
    val spark = SparkSession
    .builder()
    .master("local[3]")
    .appName("Spark_Language_Used")
    .config("hive.metastore.warehouse.uris", "thrift://localhost:9083")
    .enableHiveSupport()
    .getOrCreate()
    import spark.implicits._
    val top10 = spark.sql("select lang,count(*)as tweeters from tweets_record3 group by lang order by tweeters desc limit 10").show();
  }
}
