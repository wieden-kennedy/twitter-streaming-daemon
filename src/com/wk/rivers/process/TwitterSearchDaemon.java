package com.wk.rivers.process;

import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

import com.wk.rivers.Tweets;

public class TwitterSearchDaemon {
	private static ConfigurationBuilder mTwitterAuth;
	private Twitter mTwitter;
	
	public TwitterSearchDaemon(String consumerKey, String consumerSecret, 
			String accessToken, String accessTokenSecret, String redisURI,
			String redisKeyspace) {
		mTwitterAuth = new ConfigurationBuilder();
		mTwitterAuth.setOAuthConsumerKey(consumerKey)
		.setOAuthConsumerSecret(consumerSecret)
		.setOAuthAccessToken(accessToken)
		.setOAuthAccessTokenSecret(accessTokenSecret)
		.setJSONStoreEnabled(true);
		
		mTwitter = new TwitterFactory(mTwitterAuth.build()).getInstance();
	}
	
	public QueryResult doQuery(Query query) {
		try {
			return mTwitter.search(query);
		} catch (TwitterException te) {
            te.printStackTrace();
            System.out.println("Failed to search tweets: " + te.getMessage());
            System.exit(1);
        }
		return null;
	}
	
	private static Options setUpOptions() {
		Options opts = new Options();
		
		OptionBuilder.withLongOpt("query");
		OptionBuilder.hasArgs();
		OptionBuilder.withDescription("Twitter Search Query");
		OptionBuilder.isRequired();
		opts.addOption(OptionBuilder.create('q'));
		
		OptionBuilder.withLongOpt("auth");
		OptionBuilder.hasArgs(4);
		OptionBuilder.withValueSeparator(',');
		OptionBuilder.isRequired();
		OptionBuilder.withDescription("OAuth Credentials.");
		opts.addOption(OptionBuilder.create('a'));
		
		OptionBuilder.withLongOpt("redis");
		OptionBuilder.hasArg();
		OptionBuilder.withDescription("Redis connection URI");
		OptionBuilder.isRequired();
		opts.addOption(OptionBuilder.create('r'));
		
		OptionBuilder.withLongOpt("keyspace");
		OptionBuilder.hasArg();
		OptionBuilder.withDescription("Redis keyspace to put tweets in");
		OptionBuilder.isRequired();
		opts.addOption(OptionBuilder.create("k"));
		return opts;
	}
	
	private static void printUsage(Options options) {
		HelpFormatter help = new HelpFormatter();
		help.setWidth(80);
		help.printHelp("-a <consumerkey>,<consumersecret>,<accesstoken>,<accesstokensecret> -r <redisurl> -k <keyspace> -q <query>", options);
	}
	
	public void searchTwitter(String queryString, Tweets river) {
		QueryResult result;
		Query query = new Query(queryString);
		query.setResultType(Query.RECENT);	
	
		do {
			long max = 0;
			query.setCount(100);
			result = doQuery(query);
			
			System.out.println(result.getCount());
			List<Long> ids = new ArrayList<Long>();			
            List<Status> tweets = result.getTweets();
            for (Status tweet : tweets) {
                if (!river.contains(tweet)) {
                	river.add(tweet);
                }
                ids.add(tweet.getId());
            }
     
            max = Collections.min(ids);  
            System.out.println("max_id: "+max);
            query = new Query(queryString);
            query.setMaxId(max);
            try {
            	System.out.println("sleeping...");
				TimeUnit.SECONDS.sleep(2);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        } while (result.getCount() > 0);
	}
	
	public static void main(String[] args) {
		
		Options opts = setUpOptions();		
		CommandLineParser parser = new GnuParser();
		CommandLine commandLine;
		try {
			commandLine = parser.parse(opts, args);
			String[] auth = commandLine.getOptionValues("a");
			String redisUri = commandLine.getOptionValue("r");
			String keyspace = commandLine.getOptionValue("k");
			
			TwitterSearchDaemon daemon = new TwitterSearchDaemon(auth[0],
					auth[1], auth[2], auth[3], redisUri, keyspace);
			
			if (commandLine.hasOption("q")) {
				String[] queryStrings = commandLine.getOptionValues("q");
				String queryString = queryStrings[0];
				
				Tweets river = Tweets.on(keyspace, redisUri);
				daemon.searchTwitter(queryString, river);
			} else {
				printUsage(opts);
				System.exit(1);
			}
		} catch (ParseException e) {			
			System.out.println(e);
			printUsage(opts);
			System.exit(1);
		}
		
	}
}