
package edu.miu.bigData

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import org.apache.log4j.{Level, Logger}

object StreamTweets {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
    
    Logger.getRootLogger().setLevel(Level.WARN)
    val confi = new SparkConf().setAppName("Twittes").setMaster("local[*]")
    val sc  = new SparkContext(confi)
    val ssc = new StreamingContext(sc, Seconds(2))

    /*Twitter API*/
//    val cb = new ConfigurationBuilder
//    cb.setDebugEnabled(true)
//      .setOAuthConsumerKey("rM3mGsfnrvlHx4uiBDbn7txj5")
//      .setOAuthConsumerSecret("AHlzNWg7gxeDHMjsQwl27rwHXzxDsZme4Cipgu0TklbHPTx1M9")
//      .setOAuthAccessToken("1573431326468087808-JoiYHPgI4rGfuv3Vei4TUxGmPjJIiy")
//      .setOAuthAccessTokenSecret("CSjLsTQ9jd23VAm4yvldGR9SbTftPLn6f6YVO2Izh0xT5")
//
//    val auth = new OAuthAuthorization(cb.build)
    System.setProperty("twitter4j.oauth.consumerKey","rM3mGsfnrvlHx4uiBDbn7txj5")
    System.setProperty("twitter4j.oauth.consumerSecret", "AHlzNWg7gxeDHMjsQwl27rwHXzxDsZme4Cipgu0TklbHPTx1M9")
    System.setProperty("twitter4j.oauth.accessToken", "1573431326468087808-JoiYHPgI4rGfuv3Vei4TUxGmPjJIiy")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "CSjLsTQ9jd23VAm4yvldGR9SbTftPLn6f6YVO2Izh0xT5")
    val filters = args . takeRight(args.length - 5)
    val tweets = TwitterUtils.createStream(ssc, None,filters)
   
    val statuses = tweets.map(status => (status.getUser.getId, status.getLang,status.getUser.getFriendsCount,
      status.getUser.getLocation,status.getUser.getFollowersCount,status.getSource.split("[<,>]")(2),
      status.getRetweetCount,status.isPossiblySensitive,status.isRetweet,status.isRetweeted,status.getCreatedAt.toString,
      status.getText)).map(_.productIterator.mkString("\t"))
  
    statuses.foreachRDD { (rdd, time) =>
      rdd.foreachPartition { partitionIter =>
        val props = new Properties()
        val bootstrap = "localhost:9092" 
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("bootstrap.servers", bootstrap)
        
        val producer = new KafkaProducer[String, String](props)
        
        partitionIter.foreach { elem =>
          val dat = elem.toString()
          //Kafka topic
          val data = new ProducerRecord[String, String]("tweets", null, dat) 
          producer.send(data)
        }
        producer.flush()
        producer.close()
      }
    }
    
    ssc.start()
    ssc.awaitTermination()
  }  
}
