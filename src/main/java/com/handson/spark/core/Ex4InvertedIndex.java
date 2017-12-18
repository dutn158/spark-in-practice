package com.handson.spark.core;

import com.handson.spark.utils.Parse;
import com.handson.spark.utils.Tweet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Map;

/**
 * Buildind a hashtag search engine
 * <p>
 * The goal is to build an inverted index. An inverted index is the data structure used to build search engines.
 * <p>
 * How does it work?
 * <p>
 * Assuming #spark is an hashtag that appears in tweet1, tweet3, tweet39.
 * The inverted index that you must return should be a Map (or HashMap) that contains a (key, value) pair as (#spark, List(tweet1,tweet3, tweet39)).
 */
public class Ex4InvertedIndex {

    private static String pathToFile = "data/reduced-tweets.json";

    /**
     * Load the data from the json file and return an RDD of Tweet
     */
    public JavaRDD<Tweet> loadData() {
        // create spark configuration and spark context
        SparkConf conf = new SparkConf()
                .setAppName("Inverted index")
                .set("spark.driver.allowMultipleContexts", "true")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Tweet> tweets = sc.textFile(pathToFile)
                .map(line -> Parse.parseJsonToTweet(line));

        return tweets;
    }

    public Map<String, Iterable<Tweet>> invertedIndex() {
        JavaRDD<Tweet> tweets = loadData();

        // for each tweet, extract all the hashtag and then create couples (hashtag,tweet)
        // Hint: see the flatMapToPair method
        // TODO write code here
        JavaPairRDD<String, Tweet> pairs = null;

        // We want to group the tweets by hashtag
        // TODO write code here
        JavaPairRDD<String, Iterable<Tweet>> tweetsByHashtag = null;

        // Then return the inverted index (= a map structure)
        // TODO write code here
        Map<String, Iterable<Tweet>> map = null;

        return map;
    }

}
