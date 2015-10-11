package redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * Created by gurramvinay on 10/11/15.
 */
public class RedisClient {
  private JedisPool pool;

  public RedisClient(String host, int port){
    this.pool = new JedisPool(host,port);
  }

  public void connectionDestroy() {
    pool.destroy();
  }

  public Jedis getResource() {
    return pool.getResource();
  }

  public void returunReource(Jedis resource){
    pool.returnResourceObject(resource);
  }

}
