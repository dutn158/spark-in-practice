package com.handson.spark.core;

import com.handson.spark.utils.Parse;
import com.handson.spark.utils.Tweet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Map;

/**
 * The Java Spark API documentation: http://spark.apache.org/docs/latest/api/java/index.html
 * <p>
 * We still use the dataset with the 8198 reduced tweets. The data are reduced tweets as the example below:
 * <p>
 * {"id":"572692378957430785",
 * "user":"Srkian_nishu :)",
 * "text":"@always_nidhi @YouTube no i dnt understand bt i loved of this mve is rocking",
 * "place":"Orissa",
 * "country":"India"}
 * <p>
 * We want to make some computations on the users:
 * - find all the tweets by user
 * - find how many tweets each user has
 * <p>
 * Use the Ex1UserMiningTest to implement the code.
 */
public class Ex1UserMining {

    private static String pathToFile = "data/reduced-tweets.json";

    /**
     * Load the data from the json file and return an RDD of Tweet
     */
    public JavaRDD<Tweet> loadData() {
        // Create spark configuration and spark context
        SparkConf conf = new SparkConf()
                .setAppName("User mining")
                .set("spark.driver.allowMultipleContexts", "true")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load the data and parse it into a Tweet.
        // Look at the Tweet Object in the TweetUtils class.
        JavaRDD<Tweet> tweets = sc.textFile(pathToFile)
                .map(line -> Parse.parseJsonToTweet(line));

        return tweets;
    }

    /**
     * For each user return all his tweets
     */
    public JavaPairRDD<String, Iterable<Tweet>> tweetsByUser() {
        JavaRDD<Tweet> tweets = loadData();

        // TODO write code here
        // Hint: the Spark API provides a groupBy method
        JavaPairRDD<String, Iterable<Tweet>> tweetsByUser = tweets.mapToPair(tweet -> new Tuple2<>(tweet.getUser(), tweet)).groupByKey();

        return tweetsByUser;
    }

    /**
     * Compute the number of tweets by user
     */
    public JavaPairRDD<String, Integer> tweetByUserNumber() {
        JavaRDD<Tweet> tweets = loadData();

        // TODO write code here
        // Hint: think about what you did in the wordcount example
        JavaPairRDD<String, Integer> count =
                tweets.mapToPair(tweet -> new Tuple2<>(tweet.getUser(), 1)).reduceByKey((integer, integer2) -> integer + integer2);

        return count;
    }

    public static void main(String[] args) {
        Ex1UserMining ex1UserMining = new Ex1UserMining();
        JavaPairRDD<String, Iterable<Tweet>> a1 = ex1UserMining.tweetsByUser();
        Map<String, Iterable<Tweet>> res = a1.collectAsMap();
        System.out.println(res.size());
    }

}
