package redis;

import redis.clients.jedis.Jedis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Publisher
{
    private final Jedis publisherJedis ;

    public Publisher(Jedis publisherJedis)
    {
        this.publisherJedis = publisherJedis;
    }
    public void start(String channel_name)
    {
        System.out.println("Type your message....exit for terminate");
        try
        {
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

            while (true)
            {
                String line = reader.readLine();
                if (!"exit".equals(line)) {
                    publisherJedis.publish(channel_name, line);
                }
                else {
                    break;
                }
            }
        }
        catch (IOException e) {
            System.out.println("IO failure while reading input, e");
        }
    }
}
