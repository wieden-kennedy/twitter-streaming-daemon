package com.wk.rivers;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

import twitter4j.ConnectionLifeCycleListener;
import twitter4j.FilterQuery;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.StatusListener;

import com.wk.rivers.TwitterStreamDaemonStatusListener;

public class TwitterStreamDaemon implements ConnectionLifeCycleListener {
	private CommandLine mCommandLine;
	
	private TwitterStream mStream;
	private FilterQuery mFilter;
	private String[] mTrack;
	private long[] mFollow;
	
	private ConfigurationBuilder auth;
	
	public TwitterStreamDaemon(String consumerKey, String consumerSecret, String accessToken, String accessTokenSecret) {
		auth = new ConfigurationBuilder();
		auth.setOAuthConsumerKey(consumerKey)
		.setOAuthConsumerSecret(consumerSecret)
		.setOAuthAccessToken(accessToken)
		.setOAuthAccessTokenSecret(accessTokenSecret);
	}

	public TwitterStream buildStream() {	
		TwitterStream stream = new TwitterStreamFactory(auth.build()).getInstance();
		//LOL @ class name (my actual life)
		StatusListener listener = new TwitterStreamDaemonStatusListener();
		stream.addListener(listener);
		// let's try to handle disconnects (and connects?)
		stream.addConnectionLifeCycleListener(this);
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
		setUp();
		
	}
	
	private static void setUp() {
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
		OptionBuilder.withDescription("OAuth Credentials.");
		
	}

	@Override
	public void onCleanUp() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onConnect() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onDisconnect() {
		// TODO Auto-generated method stub
		stop();
		System.exit(1);
	}
}

