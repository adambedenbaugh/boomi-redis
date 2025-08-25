package com.boomi.proserv.caching.impl;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.*;

import java.util.*;
import java.util.logging.Logger;

/**
 * Class Wrapper with Jedis Implementation
 * 
 * @author anthony.rabiaza@gmail.com
 *
 */
public class CacheJedisWrapper {

	private static int S_TIMEOUT = 0;
	private static int S_ATTEMPS = 3;
	private static final Logger logger = Logger.getLogger(CacheJedisWrapper.class.getName());

	Jedis jedis;
	JedisCluster jedisCluster;
	JedisPool jedisPool;
	JedisClusterConnectionHandler jedisConnHandler;
	
	boolean poolEnabled;

	public CacheJedisWrapper(String hosts, String user, String password, boolean useSSL, String parameters, boolean poolEnabled, int poolSize) {
		this.poolEnabled = poolEnabled;

		if (hosts == null || hosts.isEmpty()) {
        	Utils.throwException(new Exception("Host is empty"));
        return;
    	}

		if(hosts != null && hosts.length()>0) {
			//Cluster
			if(hosts.contains(",")) {
				// getLogger().info("Connecting to Redis with hosts list: " + hosts);
	
				String[] pairs = hosts.split(",");
				Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
				for(int i=0;i<pairs.length;i++) {
					String[] pair = pairs[i].split(":");
					jedisClusterNodes.add(new HostAndPort(pair[0], Integer.parseInt(pair[1])));
				}

				if(password != null && password.length()>0) {
					// logger.info("Creating Redis with hosts list: " + hosts);
					if(poolEnabled) {
						GenericObjectPoolConfig<Jedis> poolConfig = new GenericObjectPoolConfig<>();
						poolConfig.setMaxWait(java.time.Duration.ofMillis(S_TIMEOUT));
						poolConfig.setMaxTotal(poolSize);
						
						jedisCluster = new JedisCluster(
							jedisClusterNodes,
							S_TIMEOUT,
							S_TIMEOUT,
							S_ATTEMPS,
							user,
							password,
							null,
							poolConfig,
							useSSL
						);
						
					} else {
						// logger.info("Creating Redis with hosts list and no pooling");
						
						jedisCluster = new JedisCluster(
							jedisClusterNodes,
							S_TIMEOUT,
							S_TIMEOUT,
							S_ATTEMPS,
							user,
							password,
							null,
							new GenericObjectPoolConfig<Jedis>(),
							useSSL
						);
						
					}
				} else {
					// logger.fine("Connection to Redis with no password");
					if(poolEnabled) {
						GenericObjectPoolConfig<Jedis> poolConfig = new GenericObjectPoolConfig<>();
						poolConfig.setMaxWait(java.time.Duration.ofMillis(S_TIMEOUT));
						poolConfig.setMaxTotal(poolSize);
						GenericObjectPoolConfig<Jedis> config = poolEnabled ? poolConfig : new GenericObjectPoolConfig<>();
						
						if (!useSSL) {
							jedisCluster = new JedisCluster(
								jedisClusterNodes, 
								S_TIMEOUT, 
								S_TIMEOUT, 
								S_ATTEMPS,
								config
							);
						} else {
							jedisCluster = new JedisCluster(
								jedisClusterNodes,
								S_TIMEOUT,
								S_TIMEOUT,
								S_ATTEMPS,
								null,
								null, 
								config,
								true
							);
						}
					}
				}

				jedisConnHandler = null;

				//getLogger().info("Done creating Redis with hosts list");

			} 
			//Single Node
			else {
				//getLogger().info("Trying to create Redis with single host " + hosts);
				String[] pair = hosts.split(":");

				if(pair.length != 2) {
					Utils.throwException(new Exception("Invalid Redis Host: " + hosts + ". Syntax is <host>:<port>"));
				}
		        
				if(poolEnabled) {
					getLogger().info("Creating Redis with single host and pool");
					JedisPoolConfig poolConfig = new JedisPoolConfig();
					poolConfig.setMaxWait(java.time.Duration.ofMillis(S_TIMEOUT));
					poolConfig.setMaxTotal(poolSize);
					
					if(password != null && password.length()>0) {
						jedisPool = new JedisPool(
							poolConfig, 
							pair[0], 
							Integer.parseInt(pair[1]), 
							S_TIMEOUT,
							user,
							password, 
							useSSL
						);
					} else {
						jedisPool = new JedisPool(
							poolConfig, 
							pair[0], 
							Integer.parseInt(pair[1]), 
							S_TIMEOUT, 
							useSSL
						);
					}
				} else {
					getLogger().info("Creating Redis with single host with pool disabled");
					if (password != null && password.length() > 0) {
						jedis = new Jedis( 
							pair[0], 
							Integer.parseInt(pair[1]),
							S_TIMEOUT,
							useSSL);
						jedis.auth(user, password);

					} else {
						jedis = new Jedis(
							pair[0], 
							Integer.parseInt(pair[1]), 
							S_TIMEOUT, 
							useSSL
						);
					}
				}
				//getLogger().info("Done creating Redis with single host");
				jedisCluster = null;
			}
		} else {
			Utils.throwException(new Exception("Cache Host is empty"));
		}
	}

	public boolean isValid() {
		try {
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	private boolean isCluster() {
		return jedisCluster!=null;
	}

	private Jedis getJedis() {
		if(poolEnabled && jedisPool != null) {
			return jedisPool.getResource();
		} else {
			return jedis;
		}
	}
	
	private void releaseJedis(Jedis jedis) {
		if(jedis != null) {
			jedis.disconnect();
			jedis.close();
		}
	}
	
	public Map<String, String> getAll(String hashVal, Long ttl) {
		getLogger().fine("getAll with args hashVal, ttl : " + hashVal + ", " + ttl);
		HashMap<String, String> result = new HashMap<String, String>();
		Map<String, Response<String>> responses = new HashMap<>();
		List<String> keys;
		Pipeline p;
		ScanResult<String> scanResult;
		Jedis jedis = null;

		try {
			ScanParams scanParams = new ScanParams().count(100).match(hashVal);
			String cur = redis.clients.jedis.ScanParams.SCAN_POINTER_START;
			do {
				if(jedisCluster!=null) {
					scanResult = jedisCluster.scan(cur, scanParams);
					keys = scanResult.getResult();

					for (String thisKey : keys) {
						result.put(thisKey, jedisCluster.get(thisKey));
					}
				} else {
					jedis = getJedis();
					scanResult = jedis.scan(cur, scanParams);
					keys = scanResult.getResult();
					p = jedis.pipelined();
					// work with result
					for (String thisKey : keys) {
						responses.put(thisKey, p.get(thisKey));
					}
					p.sync();

					for (String thisKey : responses.keySet()) {
						Response<String> r = (Response<String>) responses.get(thisKey);
						result.put(thisKey, r.get());
						//getLogger().finest("retrieved key: " + thisKey + " and value: " + r.get());
					}
				}
				cur = scanResult.getCursor();
			} while (!cur.equals(redis.clients.jedis.ScanParams.SCAN_POINTER_START));
		} finally {
			if(jedis!=null) {
				releaseJedis(jedis);
			}
		}

		return result;
	}

	public String get(String hashVal, Long ttl) {
		String result;
		if(isCluster()) {
			result = jedisCluster.get(hashVal);
			if (ttl != -1) {
				jedisCluster.pexpire(hashVal, ttl);
			}
		} else {
			Jedis jedis = getJedis();
			try {
				result = jedis.get(hashVal);
				if (ttl != -1) {
					jedis.pexpire(hashVal, ttl);
				}
			} finally {
				releaseJedis(jedis);
			}
		}

		return result;
	}

	public void set(String hashVal, String value, Long ttl) {
		if(isCluster()) {
			if (ttl != -1) {
				jedisCluster.psetex(hashVal, ttl, value);
			} else {
				jedisCluster.set(hashVal, value);
			}
		} else {
			Jedis jedis = getJedis();
			try {
				if (ttl != -1) {
					jedis.psetex(hashVal, ttl, value);
				} else {
					jedis.set(hashVal, value);
				}
			} finally {
				releaseJedis(jedis);
			}
		}
	}

	public void del(String hashVal) {
		if(isCluster()) {
			jedisCluster.del(hashVal);
		} else {
			Jedis jedis = getJedis();
			try {
				jedis.del(hashVal);
			} finally {
				releaseJedis(jedis);
			}
		}
	}

	public void delAll(String hashVal) {
		getLogger().fine("delAll with args hashVal: " + hashVal);
			Jedis jedis = null;
			long numDel;
			try {
				ScanParams scanParams = new ScanParams().count(100).match(hashVal);
				String cur = redis.clients.jedis.ScanParams.SCAN_POINTER_START;
				ScanResult<String> scanResult;
				do {
					if(jedisCluster!=null) {
						scanResult = jedisCluster.scan(cur, scanParams);
					} else {
						jedis = getJedis();
						scanResult = jedis.scan(cur, scanParams);
					}
					String[] arrKeys = Arrays.copyOf(scanResult.getResult().toArray(), scanResult.getResult().size(), String[].class);
					if (arrKeys.length > 0) {
						getLogger().fine("Attempting to del " + arrKeys.length + " keys");
						if(jedisCluster!=null) {
							numDel = jedisCluster.del(arrKeys);
						} else {
							numDel = jedis.del(arrKeys);
						}
						if (numDel != arrKeys.length) {
							getLogger().warning("Could not delete " + (arrKeys.length - numDel) + " entries from Redis");
						}
					} else {
						getLogger().fine("Nothing to delete");
					}

					cur = scanResult.getCursor();
				} while (!cur.equals(redis.clients.jedis.ScanParams.SCAN_POINTER_START));
			} finally {
				if(jedis!=null) {
					releaseJedis(jedis);
				}
			}
	}
	
	private Logger getLogger() {
		return Logger.getLogger(this.getClass().getName());
	}

}

class Utils
{
	@SuppressWarnings("unchecked")
	private static <T extends Throwable> void throwException(Throwable exception, Object dummy) throws T
	{
		throw (T) exception;
	}

	public static void throwException(Throwable exception)
	{
		Utils.<RuntimeException>throwException(exception, null);
	}
}