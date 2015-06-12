package com.duchessfr.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Tuple2;
import twitter4j.Status;

import java.util.ArrayList;
import java.util.List;

/**
 *  The Spark Streaming documentation is available on:
 *  http://spark.apache.org/docs/latest/streaming-programming-guide.html
 *
 *  Spark Streaming is an extension of the core Spark API that enables scalable,
 *  high-throughput, fault-tolerant stream processing of live data streams.
 *  Spark Streaming receives live input data streams and divides the data into batches,
 *  which are then processed by the Spark engine to generate the final stream of results in batches.
 *  Spark Streaming provides a high-level abstraction called discretized stream or DStream,
 *  which represents a continuous stream of data.
 *
 *  In this exercise we will:
 *  - Print the status of each tweet
 *  - Find the 10 most popular Hashtag
 *
 *  You can see informations about the streaming in the Spark UI console: http://localhost:4040/streaming/
 */
public class StreamingOnTweets {

  JavaStreamingContext jssc;
  /**
   *  Load the data using TwitterUtils: we obtain a DStream of tweets
   *
   *  More about TwitterUtils:
   *  https://spark.apache.org/docs/1.4.0/api/java/index.html?org/apache/spark/streaming/twitter/TwitterUtils.html
   */
  public JavaDStream<Status> loadData() {
    // create the spark configuration and spark context
    SparkConf conf = new SparkConf()
        .setAppName("Play with Spark Streaming")
        .setMaster("local[*]");

    // create a java streaming context and define the window
    jssc = new JavaStreamingContext(conf, Durations.seconds(2));

    System.out.println("Initializing Twitter stream...");

    // create a DStream (sequence of RDD):
    // - the Status class contains all information of a tweet
    // See http://twitter4j.org/javadoc/twitter4j/Status.html
    JavaDStream<Status> tweetsStream = TwitterUtils.createStream(jssc, StreamUtils.getAuth());

    return tweetsStream;

  }

  /**
   *  Print the status's text of each status
   */
  public void tweetPrint() {
    JavaDStream<Status> tweetsStream = loadData();

    // Here print the status's text: see the Status class
    // Hint: use the print method
    JavaDStream<String> statusText = null;

    // Start the context
    jssc.start();
    jssc.awaitTermination();
  }

  /**
   *  Find the 10 most popular Hashtag
   */
  public String popularHashtag() {
    JavaDStream<Status> tweetsStream = loadData();

    // First, find all hashtags
    JavaDStream<String> hashtags = null;

    // Make a "wordcount" on hashtag
    // Hint: define a window for the reduce step. For example 50000 milliseconds.
    JavaPairDStream<Integer, String> hashtagMention = null;


    // Then sort the hashtags
    // Hint: look at the transformToPair method
    JavaPairDStream<Integer, String> sortedHashtag = null;

    // and return the 10 most populars
    List<Tuple2<Integer, String>> mostPopulars = new ArrayList<>();
;   // Hint: loop on the RDD and take the 10 most popular

    // Start the context
    jssc.start();
    jssc.awaitTermination();

    return "Most popular hashtag :" + mostPopulars;
  }

}