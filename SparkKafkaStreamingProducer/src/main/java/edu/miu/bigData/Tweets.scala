
package edu.miu.bigData

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import org.apache.log4j.{Level, Logger}

object SparkStreamTweets {
 
  def main(args: Array[String]) {

        Logger.getRootLogger().setLevel(Level.WARN)
        //App1
//    System.setProperty("twitter4j.oauth.consumerKey", "a3Mycy3NCjnG5MgSt1wdGXDAu")
//    System.setProperty("twitter4j.oauth.consumerSecret", "0ODHO1Oe78MDU4VsLFMMiIu25SdKZWQ3OtUHyZ2fIY9Lh1FX32")
//    System.setProperty("twitter4j.oauth.accessToken", "1573431326468087808-tI6f9aN8Z9I35e4AVie2NGLDiRDA6Z")
//    System.setProperty("twitter4j.oauth.accessTokenSecret", "c2hcpeCsljIk2YUBb3Uyu5WkmFg88gv1C0qmuGyj7TnfG")

//App2
//    System.setProperty("twitter4j.oauth.consumerKey", "pdaAbASjgUNP3Yxll366iL79i")
//    System.setProperty("twitter4j.oauth.consumerSecret", "BRPoEQ16LyKoBMr9qWdkLabz8jAh00cSKLgKy3qXja2B7MHMzr")
//    System.setProperty("twitter4j.oauth.accessToken", "1573431326468087808-fdhdGfiyvTHOBBIgtv0jXC0zdofmzN")
//    System.setProperty("twitter4j.oauth.accessTokenSecret", "cDvZsGhUE0L9TcFAkDIlN80OjEtM5qsp7D99ogTcV1b5G")  
//    
//    val tweets = TwitterUtils.createStream(ssc, None)
   val conf1 = new SparkConf()
      .setAppName("Tweet Producer")
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq).setMaster("local[*]")

    val ssc1 = new StreamingContext(conf1, Seconds(1))
    import twitter4j.TwitterFactory
    val cb = new ConfigurationBuilder()
    cb.setOAuthConsumerKey("a3Mycy3NCjnG5MgSt1wdGXDAu")
    cb.setOAuthConsumerSecret("0ODHO1Oe78MDU4VsLFMMiIu25SdKZWQ3OtUHyZ2fIY9Lh1FX32")
    cb.setOAuthAccessToken("1573431326468087808-tI6f9aN8Z9I35e4AVie2NGLDiRDA6Z")
    cb.setOAuthAccessTokenSecret("c2hcpeCsljIk2YUBb3Uyu5WkmFg88gv1C0qmuGyj7TnfG")

    val twitterAuth = new TwitterFactory(cb.build()).getInstance().getAuthorization()

    val tweetStream = TwitterUtils.createStream(ssc1, Some(twitterAuth), Array(""))
    
    val statuses = tweetStream.map(status => (status.getUser.getId,status.getUser.getFriendsCount,
      status.getUser.getLocation,status.getUser.getFollowersCount,status.getSource.split("[<,>]")(2),
      status.getRetweetCount,status.isPossiblySensitive,status.isRetweet,status.getCreatedAt.toString,
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
          val data = new ProducerRecord[String, String]("tweets", null, dat) 
          producer.send(data)
        }
        producer.flush()
        producer.close()
      }
    }
    
    ssc1.start()
    ssc1.awaitTermination()
  }  
}
