package redis;

import com.olastore.listing.consumers.scheduler.Subscriber;
import com.olastore.listing.consumers.utils.AppConfigFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.util.*;

public class RedisPubSub {
  public static final Logger logger = LoggerFactory.getLogger(RedisPubSub.class);

  public static List<String> getEvents(String topicName) {

      return new ArrayList<>();
  }

    public static void addSubscriber(final String channelName, final Subscriber subscriber) {
        JedisPool jedispool = new JedisPool((String) AppConfigFinder.get("redis_host"));
        final Jedis subscriberJedis = jedispool.getResource();
        new Thread(new Runnable(){
            public void run() {
                try {
                    logger.info("Subscribing to " + channelName);
                    subscriberJedis.subscribe(subscriber,channelName);
                    logger.info("Subscribing ended");
                }
                catch (Exception e) {
                    logger.error("Subscribing failed "+e.getMessage());
                }
            }
        }).start();
    }
}
