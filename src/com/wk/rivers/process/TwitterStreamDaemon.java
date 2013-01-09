package com.wk.rivers.process;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import twitter4j.FilterQuery;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.StatusListener;

import com.wk.rivers.process.TwitterStreamDaemonStatusListener;

public class TwitterStreamDaemon {
	
	private TwitterStream mStream;
	private FilterQuery mFilter;
	private String[] mTrack;
	private long[] mFollow;
	private String mRedisURI;
	private String mRedisKeyspace;
	private ConfigurationBuilder mTwitterAuth;
	
	public TwitterStreamDaemon(String consumerKey, String consumerSecret,
			String accessToken, String accessTokenSecret, String redisURI,
			String redisKeyspace) {
		mTwitterAuth = new ConfigurationBuilder();
		mTwitterAuth.setOAuthConsumerKey(consumerKey)
		.setOAuthConsumerSecret(consumerSecret)
		.setOAuthAccessToken(accessToken)
		.setOAuthAccessTokenSecret(accessTokenSecret);
		
		mRedisURI = redisURI;
		mRedisKeyspace = redisKeyspace;
	}

	public TwitterStream buildStream() {	
		TwitterStream stream = new TwitterStreamFactory(mTwitterAuth.build()).getInstance();
		//LOL @ class name (my actual life)
		StatusListener listener = new TwitterStreamDaemonStatusListener(mRedisURI, mRedisKeyspace);
		stream.addListener(listener);
		// let's try to handle disconnects (and connects?)
		return stream;
	}

	public FilterQuery buildFilterQuery(String[] track) {
		return buildFilterQuery(track, new long[0]);
	}
	
	public FilterQuery buildFilterQuery(String[] track, long[] follow) {
		FilterQuery query = new FilterQuery();
		query.track(track);
		query.follow(follow);
		return query;		
	}
	
	public void start() {
		mStream = buildStream();
		mFilter = buildFilterQuery(mTrack, mFollow);
		
		if (mStream != null && mFilter != null) {
			mStream.filter(mFilter);
		}
	}

	public void stop() {
		mStream.shutdown();
		mStream.cleanUp();
	}
	
	public void setTrack(String[] track) {
		mTrack = track;
	}
	
	public void setFollow(String[] follow) {
		long[] followlong = new long[follow.length];
		
		int i = 0;
		for (String id : follow) {
			followlong[i] = new Long(id);
			i++;
		}
		mFollow = followlong;
	}
	
	public static void main(String[] args) {
		//Given OAuth creds, redis keyspace, filter params etc.... start streaming
		Options opts = setUpOptions();
		
		CommandLineParser parser = new GnuParser();
		CommandLine commandLine;
		try {
			commandLine = parser.parse(opts, args);
			String[] auth = commandLine.getOptionValues("a");
			String redisUri = commandLine.getOptionValue("r");
			String keyspace = commandLine.getOptionValue("k");
			
			TwitterStreamDaemon daemon = new TwitterStreamDaemon(auth[0],
					auth[1], auth[2], auth[3], redisUri, keyspace);
			
			if (commandLine.hasOption("t")) {
				String[] track = commandLine.getOptionValues("t");
				daemon.setTrack(track);
			} 
			if (commandLine.hasOption("f")) {
				String[] follow = commandLine.getOptionValues("f");
				daemon.setFollow(follow);
			}
			daemon.start();
		} catch (ParseException e) {
			printUsage(opts);
			System.exit(1);
		}	
	}
	
	private static Options setUpOptions() {
		Options opts = new Options();
		
		OptionBuilder.withLongOpt("track");
		OptionBuilder.hasArgs();
		OptionBuilder.withValueSeparator(',');
		OptionBuilder.withDescription("Terms to track.");
		opts.addOption(OptionBuilder.create('t'));
		
		OptionBuilder.withLongOpt("follow");
		OptionBuilder.hasArgs();
		OptionBuilder.withValueSeparator(',');
		OptionBuilder.withDescription("User IDs to follow");
		opts.addOption(OptionBuilder.create('f'));
		
		OptionBuilder.withLongOpt("auth");
		OptionBuilder.hasArgs(4);
		OptionBuilder.withValueSeparator(',');
		OptionBuilder.isRequired();
		OptionBuilder.withDescription("OAuth Credentials.");
		opts.addOption(OptionBuilder.create('a'));
		
		OptionBuilder.withLongOpt("redis");
		OptionBuilder.hasArg();
		OptionBuilder.withDescription("Redis connection URI");
		opts.addOption(OptionBuilder.create('r'));
		
		OptionBuilder.withLongOpt("keyspace");
		OptionBuilder.hasArg();
		OptionBuilder.withDescription("Redis keyspace to put tweets in");
		opts.addOption(OptionBuilder.create("k"));
		return opts;
	}
	
	private static void printUsage(Options options) {
		HelpFormatter help = new HelpFormatter();
		help.setWidth(80);
		help.printHelp("-a <consumerkey>,<consumersecret>,<accesstoken>,<accesstokensecret> -r <redisurl> -k <keyspace> [-f <userid1>,... | -t <term1>,<term2>]", options);
	}
}

