
package edu.miu.bigData

import java.util.Properties
import java.io.File
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka._
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode


object SparkTweetsConsumer {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
    
    //Logger
    Logger.getRootLogger().setLevel(Level.WARN) 
    
    val confi = new SparkConf().setAppName("PrintTweets").setMaster("local[*]")
    
    val sc = new SparkContext(confi)
    
    val ssc = new StreamingContext(sc, Seconds(1))
    
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")
    val topics = List("tweets").toSet
    val tweets = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics).map(_._2)
    .map(twt=>twt.split("\t"))
    .map(splt=>(splt(0).toLong,splt(1),splt(2).toInt,splt(3),splt(4).toInt,splt(5),splt(6).toInt,splt(7).toBoolean,splt(8).toBoolean,splt(9).toBoolean,splt(10),splt(11)))
    
    tweets.foreachRDD((rdd,time)=>{         
      val spark = SparkSession.builder().appName("Spark Tweet Hive").master("local[*]")
      .config("hive.metastore.warehouse.uris","thrift://localhost:9083") 
      .enableHiveSupport() .getOrCreate()
 
      import spark.implicits._
      import spark.sql
      
      val tweetrepartitioned = rdd.repartition(1).cache()
      //spark.sqlContext.sql("CREATE TABLE IF NOT EXISTS tweets_record (userId BIGINT, lang STRING, friendsCount INT, Location STRING,followersCount INT, deviceUsed STRING, retweetCount INT, isSensitive BOOLEAN, isRetweet BOOLEAN, isRetweeted BOOLEAN, postDate STRING,text STRING)")
      val tweetDataFrame = tweetrepartitioned.map(twt => Record(twt._1, twt._2,twt._3, twt._4,twt._5, twt._6,twt._7, twt._8,twt._9, twt._10,twt._11, twt._12)).toDF()
      //spark.sqlContext.sql("create table tweets_record as select * from tweettable")
      val sqlContext1 = new HiveContext(spark.sparkContext)
      import sqlContext1.implicits._
      tweetDataFrame.write.mode(SaveMode.Append).saveAsTable("tweets_record")
     
      tweetDataFrame.createOrReplaceTempView("tweettable")
      val tables = spark.sqlContext.sql("select * from tweettable")
      println(s"========= $time =========")
      tables.show()
    })

    ssc.checkpoint("checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }  
}
//(status.getUser.getId,status.getLang,status.getSource,status.getUser.getName,status.getCreatedAt.toString,status.getText)
case class Record(userId: Long, lang:String,friendsCount:Int, Location:String,followersCount:Int, deviceUsed:String, 
    retweetCount:Int, isSensitive:Boolean, isRetweet:Boolean, isRetweeted:Boolean, postDate:String,text:String)