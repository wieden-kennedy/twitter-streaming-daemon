package com.wk.rivers;

import net.sf.json.JSONObject;

import twitter4j.Status;

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
	
	public void add(Status status) {
		Double rank = new Double(status.getId());
		String member = toJson(status).toString();
		this.add(rank, member, new Long(status.getId()).toString());
	}
	
	private JSONObject toJson(Status status) {
		JSONObject json = JSONObject.fromObject(status);
		return json;
	}
}
