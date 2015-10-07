package redis;

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
  public static Map yamlMap;
  public static final Logger logger = LoggerFactory.getLogger(RedisPubSub.class);
  public static Set<String> popularProductsSet ;

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

  //generate Hash set from the csv
  private static Set<String> generatePopularProductSet(){
    Set<String> productIdSet = new HashSet<String>();
    FileReader fileReader = null;
    BufferedReader bufferedReader = null;
    try {
      fileReader = new FileReader(new File((String)yamlMap.get("popular_products_file_path")));
      bufferedReader = new BufferedReader(fileReader);
      String line = bufferedReader.readLine();
      while ((line = bufferedReader.readLine()) != null) {
        String productId = line;
        productIdSet.add(productId);
      }
    } catch (Exception e) {
      logger.error("Error in generating popular products" +e.getMessage());
    } finally {
      try {
        fileReader.close();
        bufferedReader.close();
      } catch (Exception e) {
        logger.error(e.getMessage());
      }
    }
    return productIdSet;
  }

  public static List<String> getEvents(String topicName) {

      return new ArrayList<>();
  }
  public static void main(String[] args) throws Exception {

    readYAML();
    popularProductsSet = generatePopularProductSet();
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
