package com.wk.rivers;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.json.DataObjectFactory;

public class Tweets extends RedisModel {
	private Tweets(String river, String ks, String redisURI) {
		super(river, ks, redisURI);
	}

	public static Tweets on(String river, String redisURI) {
		return new Tweets(river, "tweets", redisURI);
	}
	
	public boolean contains(Status status) {
		return this.contains(new Long(status.getId()).toString());
	}
	
	public List<Status> allStatuses() {
		Set<String> statuses = all();
		List<Status> statusList = new ArrayList<Status>();
		for (String statusString : statuses) {
			statusList.add(fromJson(statusString));
		}
		return statusList;
	}
	
	public List<Status> statusPage(int pageSize, int pageNumber) {
		Set<String> statuses = page(pageSize, pageNumber);
		List<Status> statusList = new ArrayList<Status>();
		for (String statusString : statuses) {
			statusList.add(fromJson(statusString));
		}
		return statusList;
	}
	
	public void add(Status status) {
		Double rank = new Double(status.getId());
		String member = toJson(status);
		super.add(rank, member, new Long(status.getId()).toString());
	}
	
	private String toJson(Status status) {
		return DataObjectFactory.getRawJSON(status);
	}
	
	private Status fromJson(String json) {
		try {
			return DataObjectFactory.createStatus(json);
		} catch (TwitterException e) {
			return null;
		}
	}
}
