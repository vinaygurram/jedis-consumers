package redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.File;
import java.io.FileInputStream;
import java.util.Map;

public class RedisPubSub {
  public static Map yamlMap;
  public static final Logger logger = LoggerFactory.getLogger(RedisPubSub.class);

  //read yaml file to get the map
  private static void readYAML(){
    try {
      Yaml yaml = new Yaml();
      yamlMap = (Map) yaml.load(new FileInputStream(new File("src/main/resources/config.yaml")));
      logger.info("Yaml reading is complete");
    }catch (Exception e){
      logger.error("Yaml configuration reading failed");
    }
  }

  public static void main(String[] args) throws Exception {

    readYAML();
    JedisPool jedispool = new JedisPool((String)yamlMap.get("redis_host"));
    final Jedis subscriberJedis = jedispool.getResource();
    final Subscriber subscriber = new Subscriber();
    new Thread(new Runnable(){
      public void run() {
        try {
          String channel_name = (String) yamlMap.get("redis_channel_name");
          logger.info("Subscribing to "+channel_name);
          subscriberJedis.subscribe(subscriber,channel_name);
          logger.info("Subscribing ended");
        }
        catch (Exception e) {
          logger.error("Subscribing failed "+e.getMessage());
        }
      }
    }).start();
    Jedis publisherJedis = jedispool.getResource();
    new Publisher(publisherJedis).start((String)yamlMap.get("redis_channel_name"));
    subscriber.unsubscribe();
    jedispool.returnResource(subscriberJedis);
    jedispool.returnResource(publisherJedis);
    jedispool.returnResource(subscriberJedis);
  }
}
