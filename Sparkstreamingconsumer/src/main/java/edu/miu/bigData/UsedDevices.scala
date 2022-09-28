
package edu.miu.bigData

import org.apache.spark.sql.SparkSession

object UsedDevices {

  def main(args: Array[String]) {
    val spark = SparkSession
    .builder()
    .master("local[3]")
    .appName("Spark_Language_Used")
    .config("hive.metastore.warehouse.uris", "thrift://localhost:9083")
    .enableHiveSupport()
    .getOrCreate()
    import spark.implicits._
    val devicesUsed = spark.sql("select lang as LanguageUsed, devicesused,count(*)as NumberOfUsers from tweets myRecord where lang in('en','it','fr')and devicesused in ('Twitter Web App','Twitter for iPhone') group by lang,devicesused order by lang.devicesused desc").show();
  }
}
