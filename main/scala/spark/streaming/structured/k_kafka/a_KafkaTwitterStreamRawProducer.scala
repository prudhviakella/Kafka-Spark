package spark.streaming.structured.k_kafka
import java.util.Properties

import twitter4j.{FilterQuery, StallWarning, Status, StatusDeletionNotice, StatusListener, TwitterStreamFactory}
import twitter4j.conf.ConfigurationBuilder
import org.apache.kafka.clients.producer._
/*
*Initially test with kafka console consumer using below command
* bin\windows\kafka-console-consumer --bootstrap-server localhost:9092 --topic Rawtwitterdata --from-beginning
 */
object a_KafkaTwitterStreamRawProducer {
  def main(args:Array[String]): Unit ={
    //Defining the kafka producer Properties
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    //Creating Kafka Producer Object
    val producer = new KafkaProducer[String, String](props)
    //Creating ConfigurationBuilder which hold the consumerKey,Secret,AccessToken,Key of twitter account
    //which will be used for authentication and authorization
    val cb = new ConfigurationBuilder();
    cb.setDebugEnabled(true).setOAuthConsumerKey("FFBW9PQKUM8VtEy0s28uWEOE1")
      .setOAuthConsumerSecret("IKxDk2X0fUGKjCsLMRGtezQ95UJ3JO7UEPnMEeiIBcXst1pVof")
      .setOAuthAccessToken("988237601625686016-XFfcZFRrKb8foSUSN5IWmzlff7EGXQ5")
      .setOAuthAccessTokenSecret("daXdevb9ydZLHeYVuzCmzg87Tw5m5j85l7McfLzkKUV6y");
    //Create a Twitter Stream with configuration builder
    val twitterStream = new TwitterStreamFactory(cb.build())
      .getInstance();
    //Creating a listener for stream
    val listener = new StatusListener() {
      //When ever user posts the new message or when ever tacker matches with any of new messages in twitter
      //that will be tracked through status.
      def onStatus(status:Status) {
        println("@" + status.getUser().getScreenName() + " - " + status.getText());
        //Creating KafkaRecord
        val record = new ProducerRecord[String, String]("Rawtwitterdata",status.getUser().getScreenName() , status.getText())
        //Pushing KafkaRecord to Topic(Cluster)
        producer.send(record)
      }
      def onDeletionNotice( statusDeletionNotice:StatusDeletionNotice) {
        println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
      }

      def onTrackLimitationNotice( numberOfLimitedStatuses:Int) {
        println("Got track limitation notice:" + numberOfLimitedStatuses);
      }
      def onScrubGeo( userId:Long,  upToStatusId:Long) {
        println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
      }

      def onStallWarning( warning:StallWarning) {
        println("Got stall warning:" + warning);
      }
      def onException( ex:Exception) {
        ex.printStackTrace();
      }
    };
    twitterStream.addListener(listener);
    twitterStream.sample();
    //Use this if you want follow tweets of a user
    //twitterStream.filter(new FilterQuery().follow(1344951).follow(5988062))
    //Use this if you want to track a keywork.
    twitterStream.filter(new FilterQuery().track("fifa","nba","ipl"))
  }
}
