package redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisPubSub
{
    public static final String storeUpdateChannel= "cluster_store_update";

    public static void main(String[] args) throws Exception
    {      
        JedisPool jedispool = new JedisPool("localhost");
        final Jedis subscriberJedis = jedispool.getResource();
        final Subscriber subscriber = new Subscriber();
        new Thread(new Runnable(){
            public void run()
            {
                try
                {
                    System.out.println("Subscribing to " +storeUpdateChannel);
                    subscriberJedis.subscribe(subscriber,storeUpdateChannel);
                    System.out.println("Subscription ended.");
                }
                catch (Exception e)
                {
                    System.out.println("Subscribing failed."+e);
                }
            }
        }).start();
        Jedis publisherJedis = jedispool.getResource();
        new Publisher(publisherJedis).start(storeUpdateChannel);
        subscriber.unsubscribe();
        jedispool.returnResource(subscriberJedis);
        jedispool.returnResource(publisherJedis);
        jedispool.returnResource(subscriberJedis);
    }
}
