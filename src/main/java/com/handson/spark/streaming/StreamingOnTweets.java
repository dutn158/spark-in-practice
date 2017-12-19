package com.handson.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Tuple2;
import twitter4j.Status;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The Spark Streaming documentation is available on:
 * http://spark.apache.org/docs/latest/streaming-programming-guide.html
 * <p>
 * Spark Streaming is an extension of the core Spark API that enables scalable,
 * high-throughput, fault-tolerant stream processing of live data streams.
 * Spark Streaming receives live input data streams and divides the data into batches,
 * which are then processed by the Spark engine to generate the final stream of results in batches.
 * Spark Streaming provides a high-level abstraction called discretized stream or DStream,
 * which represents a continuous stream of data.
 * <p>
 * In this exercise we will:
 * - Print the status text of the some of the tweets
 * - Find the 10 most popular Hashtag in the last minute
 * <p>
 * Before starting these exercises fill the keys and tokens in the Streamutils class!
 * <p>
 * You can see informations about the streaming in the Spark UI console: http://localhost:4040/streaming/
 */
public class StreamingOnTweets {

    JavaStreamingContext jssc;

    /**
     * Load the data using TwitterUtils: we obtain a DStream of tweets
     * <p>
     * More about TwitterUtils:
     * https://spark.apache.org/docs/1.4.0/api/java/index.html?org/apache/spark/streaming/twitter/TwitterUtils.html
     */
    public JavaDStream<Status> loadData() {
        // create the spark configuration and spark context
        SparkConf conf = new SparkConf()
                .setAppName("Spark Streaming")
                .set("spark.driver.allowMultipleContexts", "true")
                .setMaster("local[*]");

        // create a java streaming context and define the window (2 seconds batch)
        jssc = new JavaStreamingContext(conf, Durations.seconds(2));

        System.out.println("Initializing Twitter stream...");

        // create a DStream (sequence of RDD). The object tweetsStream is a DStream of tweet statuses:
        // - the Status class contains all information of a tweet
        // See http://twitter4j.org/javadoc/twitter4j/Status.html
        // and fill the keys and tokens in the Streamutils class!
        JavaDStream<Status> tweetsStream = TwitterUtils.createStream(jssc, StreamUtils.getAuth());

        return tweetsStream;

    }

    /**
     * Print the status text of the some of the tweets
     */
    public void tweetPrint() {
        JavaDStream<Status> tweetsStream = loadData();

        // Here print the status text
        // TODO write code here
        // Hint: use the print method
        JavaDStream<String> statusText = tweetsStream.flatMap(status -> Arrays.asList(status.getText()));

        statusText.print();

        // Start the context
        jssc.start();
        jssc.awaitTermination();
    }

    /**
     * Find the 10 most popular Hashtag in the last minute
     */
    public String top10Hashtag() {
        JavaDStream<Status> tweetsStream = loadData();

        // First, find all hashtags
        // TODO write code here
        JavaDStream<String> hashtags = tweetsStream.flatMap(status -> Arrays.asList(status.getText().split(" ")))
                .filter(s -> s.length() > 1 && s.startsWith("#"));

        // Make a "wordcount" on hashtag
        // Hint: define a 1 second window (millisecond) for the reduce step.
        // TODO write code here
        JavaPairDStream<Integer, String> hashtagMention = hashtags.mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((integer, integer2) -> integer + integer2)
                .mapToPair(stringIntegerTuple2 -> new Tuple2<>(stringIntegerTuple2._2, stringIntegerTuple2._1));


        // Then sort the hashtags
        // Hint: look at the transformToPair method
        // TODO write code here
        JavaPairDStream<Integer, String> sortedHashtag = null;

        // and return the 10 most populars
        // Hint: loop on the RDD and take the 10 most popular
        // TODO write code here
        List<Tuple2<Integer, String>> mostPopulars = new ArrayList<>();
//        mostPopulars.addAll(sortedHashtag.)

        // we need to tell the context to start running the computation we have setup
        // it won't work if you don't add this!
        jssc.start();
        jssc.awaitTermination();

        return "Most popular hashtag :" + mostPopulars;
    }

}
