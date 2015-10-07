package com.olastore.listing.consumers.scheduler;

import com.olastore.listing.consumers.lib.ConsumerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPubSub;

public class Subscriber extends JedisPubSub {

    private Logger logger = LoggerFactory.getLogger(Subscriber.class);
    private String consumerName;

    Subscriber(String consumerName){

        this.consumerName = consumerName;
    }

    @Override
    public void onMessage(String channel,  String message){

        try {
            (new ConsumerFactory(consumerName)).getConsumerInstance().processEvent(message);
        } catch (Exception e){
          logger.error(e.getMessage());
        }

    }
    @Override
        public void onPMessage(String pattern, String channel, String message) {
        }
    @Override
        public void onSubscribe(String channel, int subscribedChannels) {
    }
    @Override
        public void onUnsubscribe(String channel, int subscribedChannels) {
    }
    @Override
        public void onPUnsubscribe(String pattern, int subscribedChannels) {
    }
    @Override
        public void onPSubscribe(String pattern, int subscribedChannels) {
    }
}
