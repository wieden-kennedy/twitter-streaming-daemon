package com.wk.rivers;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;

public class TwitterStreamDaemonStatusListener implements StatusListener  {
	
	@Override
	public void onException(Exception arg0) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void onDeletionNotice(StatusDeletionNotice arg0) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void onScrubGeo(long arg0, long arg1) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void onStallWarning(StallWarning arg0) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void onStatus(Status arg0) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void onTrackLimitationNotice(int arg0) {
		// TODO Auto-generated method stub
		
	}
}
