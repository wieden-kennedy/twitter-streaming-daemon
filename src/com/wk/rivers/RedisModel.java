package com.wk.rivers;

import java.net.URI;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisModel {
	private int mRedisDB;
	private String mRedisURI;
	private String mRiver;
	private String mKeySpace;
	private String mType;
	private static JedisPool sPool;
	
	public RedisModel(String river, String type, String redisURI) {
		mRiver = river;
		mType = type;
		mRedisURI = redisURI;
		mKeySpace = String.format("%s:%s", mRiver, mType);
		
	}
	
	public Set<String> all() {
		return getClient().zrevrange(mKeySpace, 0, -1);
	}
	
	public boolean contains(String id) {
		return getClient().sismember(mKeySpace+":ids", id);
	}
	
	public void add(Double rank, String member, String id){
		if (getClient().sadd(mKeySpace+":ids", id) != 0) {
			getClient().zadd(mKeySpace, rank, member);
		}
	}
	
	public Long len() {
		return getClient().zcard(mKeySpace);
	}
	
	private Jedis getClient() {
		if (sPool == null) {		
			URI uri = URI.create(mRedisURI);
			if (uri.getScheme() != null && uri.getScheme().equals("redis")) {
				String host = uri.getHost();
				int port = uri.getPort();
				String password = uri.getUserInfo().split(":", 2)[1];
				mRedisDB = Integer.parseInt(uri.getPath().split("/", 2)[1]);
				sPool = new JedisPool(new JedisPoolConfig(), host, port, 0, password);
			}
		}
		Jedis client = sPool.getResource();
		client.select(mRedisDB);
		return client;
	}
}
