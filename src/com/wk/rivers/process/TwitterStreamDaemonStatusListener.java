package com.wk.rivers.process;

import com.wk.rivers.Tweets;

import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;

public class TwitterStreamDaemonStatusListener implements StatusListener  {
	
	private Tweets mTweets;
	
	public TwitterStreamDaemonStatusListener(String redisURI, String river) {		
		mTweets = Tweets.on(river, redisURI);
	}
	
	public void onException(Exception exception) {
	}
	
	public void onDeletionNotice(StatusDeletionNotice deletion) {
	}
	
	public void onScrubGeo(long arg0, long arg1) {		
	}
	
	public void onStatus(Status status) {
		push(status);
	}
	
	public void onTrackLimitationNotice(int limit) {		
	}
	
	private void push(Status status) {		
		if (!mTweets.contains(status)) {
			mTweets.add(status);
		}
	}	
}
