package com.olastore.listing.consumers.scheduler;

import com.olastore.listing.clustering.clients.ESClient;
import com.olastore.listing.clustering.utils.ConfigReader;
import com.olastore.listing.consumers.definitions.ClustersSubscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.RedisClient;
import redis.clients.jedis.Jedis;

import java.util.Set;

/**
 * Created by gurramvinay on 10/11/15.
 */
public class SubscriberLauncher {
  public static ConfigReader esConfigReader;
  public static ConfigReader redisConfigReader;
  public static Set<String> popularProductsSet;
  public static String city;
  public static String env;
  public static Logger logger = LoggerFactory.getLogger(SubscriberLauncher.class);
  public static ESClient esClient;


  public SubscriberLauncher(ConfigReader esConfigReader, ConfigReader redisConfigReader, Set<String> popularProductsSet, String city, String env){
    this.esConfigReader = esConfigReader;
    this.redisConfigReader = redisConfigReader;
    this.popularProductsSet = popularProductsSet;
    this.city = city;
    this.env = env;
    initializeESClient();
  }

  public void initializeESClient(){
    this.esClient = new ESClient((String)esConfigReader.readValue("es_host_"+env));

  }

  public void startListening(){
    try {
      RedisClient redisClient = new RedisClient((String)redisConfigReader.readValue("redis_host_"+env),(Integer)redisConfigReader.readValue("redis_port_"+env));
      final Jedis subscriberResource = redisClient.getResource();

      final ClustersSubscriber clustersSubscriber = new ClustersSubscriber();
      final String channel_name = (String)redisConfigReader.readValue("redis_channel_name");
      logger.info("Subscribing to channel {}",channel_name);
      //starting the thread here till clarification
      new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            subscriberResource.subscribe(clustersSubscriber,channel_name);
          }catch (Exception e){
            logger.error("Exception happens {}",e);
          }
        }
      }).start();

    }catch (Exception e){
      logger.error("Exception {}",e);
    }




  }
}
