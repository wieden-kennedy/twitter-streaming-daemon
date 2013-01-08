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
		// TODO Auto-generated method stub
		
	}
	
	public void onDeletionNotice(StatusDeletionNotice deletion) {
		// TODO Auto-generated method stub
		
	}
	
	public void onScrubGeo(long arg0, long arg1) {
		// TODO Auto-generated method stub
		
	}
	
	public void onStatus(Status status) {
		push(status);
	}
	
	public void onTrackLimitationNotice(int limit) {
		// TODO Auto-generated method stub
		
	}
	
	private void push(Status status) {
		if (!mTweets.contains(status)) {
			mTweets.add(status);
		}
	}	
}
