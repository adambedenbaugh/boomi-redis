// package com.boomi.proserv.caching.test;

// import java.io.FileReader;
// import java.util.Iterator;
// import java.util.Map;
// import java.util.Properties;

// import com.boomi.proserv.caching.impl.CacheInterface;
// import com.boomi.proserv.caching.impl.CacheRedis;

// /**
//  * Tester class, please run it and provide the properties using -Dproperty=value <br/>
//  * You can add:
//  * -Dcom.boomi.proserv.caching.Cache.dynamic_process_properties_filter="param_.*" to take only the HTTP Param or
//  * -Dcom.boomi.proserv.caching.Cache.dynamic_process_properties_filter="(query_.*)|(param_.*)"
//  * @author anthony.rabiaza@gmail.com
//  *
//  */
// public class CacheTester {

// 	public static void main(String[] args) {
// 		try {
// 			CacheTester.test(new CacheRedis(), "src/test/jedis.properties");
// 			//CacheTester.test(new CacheRedis(), "src/test/jedis-cluster.properties");
// 		} catch (Exception e) {
// 			throw new RuntimeException(e);
// 		}
// 	}
// 	public static void test(CacheInterface cache, String property) throws Exception {

// 		String cacheName = "TestRedis_";
// 		Properties cacheProperties = new Properties();
// 		cacheProperties.load(new FileReader(property));
// 		cache.setProperties(cacheProperties);
// 		System.out.println("Initialization of Cache " + cache.getClass().getName() + " with " + property + "...");
// 		cache.init();
// 		System.out.println("Done");

// 		Long ttl = 15000L;
// 		Properties properties = new Properties();
// 		try {
// 			properties.put("param_1", "value1");
// 			properties.put("param_2", "value2");

// 			//dataContext.storeStream(new ByteArrayInputStream("You should store this".getBytes()), properties);

// 			cache.set(cacheName, "hello", "value", ttl);

// 			System.out.println("Adding 50 objects to cache");
// 			for(int i=0;i<50;i++) {
// 				cache.set(cacheName, "hello" + i, i + "_" + Integer.toString(Integer.valueOf(String.valueOf(i), 16)), ttl);
// 				Thread.sleep(10);
// 			}

// 			System.out.println("Sleeping for 1/3 of TTL");
// 			Thread.sleep(ttl / 3);
// 			Map<String,String> map = cache.get(cacheName, ttl);

// 			System.out.println("Retrieving 50 objects from cache");
// 			for (Iterator<String> iterator = map.keySet().iterator(); iterator.hasNext();) {
// 				String key = iterator.next();
// 				String value = map.get(key);
// 				System.out.println("K:" + key + ",V:" + value);
// 			}

// 			System.out.println("Sleeping for 1/3 of TTL");
// 			Thread.sleep(ttl / 3);

// 			System.out.println("Retrieving 50 objects from cache");
// 			for(int i=0;i<50;i++) {
// 				System.out.println(
// 						"Interation " + i + ", " +
// 						"Output: "+ cache.get(cacheName, "hello" + i, ttl)
// 						);
// 				Thread.sleep(10);
// 			}

// 			System.out.println("Sleeping for 1/3 of TTL");
// 			Thread.sleep(ttl / 3);

// 			System.out.println("Retrieving 50 objects from cache");
// 			for(int i=0;i<50;i++) {
// 				System.out.println(
// 						"Interation " + i + ", " +
// 								"Output: "+ cache.get(cacheName, "hello" + i, ttl)
// 				);
// 				Thread.sleep(10);
// 			}

// 			System.out.println("Deleting from cache");
// 			cache.delete(cacheName, "hello");
// 			cache.delete(cacheName);

// //			com.boomi.proserv.caching.Cache.set(dataContext, "javaCache");
// //
// //			System.out.println(
// //					"Output: "+ com.boomi.proserv.caching.Cache.get(dataContext, "javaCache")
// //					);
// //
// //			System.out.println(
// //					"Output: "+ com.boomi.proserv.caching.Cache.get("javaCache", "hello")
// //					);
// //			System.out.println(
// //					com.boomi.proserv.caching.Cache.getInstance().computeKey(properties)
// //					);
// //			properties.put("inuser", "value3");
// //			System.out.println(
// //					com.boomi.proserv.caching.Cache.getInstance().computeKey(properties)
// //					);
// //			properties.remove("inuser");
// //			System.out.println(
// //					com.boomi.proserv.caching.Cache.getInstance().computeKey(properties)
// //					);

// 		} catch (Exception e) {
// 			e.printStackTrace();
// 		}

// 	}
// }
