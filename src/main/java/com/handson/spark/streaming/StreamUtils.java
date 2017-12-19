package com.handson.spark.streaming;

import twitter4j.auth.OAuthAuthorization;
import twitter4j.conf.ConfigurationBuilder;

/**
 * Class to authenticate with the Twitter streaming API.
 * <p>
 * Go to https://apps.twitter.com/
 * Create your application and then get your own credentials (keys and access tokens tab)
 * <p>
 * See https://databricks-training.s3.amazonaws.com/realtime-processing-with-spark-streaming.html
 * for help.
 * <p>
 * If you have the following error "error 401 Unauthorized":
 * - it might be because of wrong credentials
 * OR
 * - a time zone issue (so be certain that the time zone on your computer is the good one)
 */
public class StreamUtils {

    // TODO fill the keys and tokens
    private static String CONSUMER_KEY = "fRAi3GYyAiKVwKzh3Ko7yrXrM";
    private static String CONSUMER_SECRET = "pt0iGCxOpDPHZpgn54fkFtuxNvRrvSVE6Vby4cPnv45AX3wbR3";
    private static String ACCESS_TOKEN = "2613792114-rY5MfsQDEB7IxcNGYOwelZ33pXwNPkBHpuE7qFE";
    private static String ACCESS_TOKEN_SECRET = "5fqJdujpfihsBIvB4W9z8QJmOqH9AUVKb4zvLQ7sisiee";

    public static OAuthAuthorization getAuth() {

        return new OAuthAuthorization(
                new ConfigurationBuilder().setOAuthConsumerKey(CONSUMER_KEY)
                        .setOAuthConsumerSecret(CONSUMER_SECRET)
                        .setOAuthAccessToken(ACCESS_TOKEN)
                        .setOAuthAccessTokenSecret(ACCESS_TOKEN_SECRET)
                        .build());
    }

}
