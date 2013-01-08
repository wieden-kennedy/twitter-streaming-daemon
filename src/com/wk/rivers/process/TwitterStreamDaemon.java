package com.wk.rivers.process;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

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
	private CommandLine mCommandLine;
	
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
	
	public static void main(String[] args) {
		//Given OAuth creds, redis keyspace, filter params etc.... start streaming
		setUpOptions();
		
	}
	
	private static void setUpOptions() {
		Options opts = new Options();
		
		OptionBuilder.withLongOpt("track");
		OptionBuilder.hasArgs();
		OptionBuilder.withDescription("Terms to track.");
		opts.addOption(OptionBuilder.create('t'));
		
		OptionBuilder.withLongOpt("follow");
		OptionBuilder.hasArgs();
		OptionBuilder.withDescription("User IDs to follow");
		opts.addOption(OptionBuilder.create('f'));
		
		OptionBuilder.withLongOpt("auth");
		OptionBuilder.hasArgs(4);
		OptionBuilder.isRequired();
		OptionBuilder.withDescription("OAuth Credentials.");
		opts.addOption(OptionBuilder.create('a'));
		
		OptionBuilder.withLongOpt("redis");
		OptionBuilder.hasArg();
		OptionBuilder.withDescription("Redis connection URI");
		opts.addOption(OptionBuilder.create('r'));
	}

}

