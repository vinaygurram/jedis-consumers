package com.olastore.listing.consumers.scheduler;

import com.olastore.listing.consumers.lib.ConsumerFactory;
import com.olastore.listing.consumers.utils.AppConfigFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.RedisPubSub;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * Created by meetanshugupta on 07/10/15.
 */
public class EventsManager {

    private String consumerName;
    private int parallelCalls;
    public static final Logger logger = LoggerFactory.getLogger(EventsManager.class);

    public EventsManager(String consumerName){
        this.consumerName = consumerName;
        this.parallelCalls = (Integer) AppConfigFinder.get(consumerName + "_PARALLEL_CALLS");

    }

    public void execute() {

//        ExecutorService executorService = Executors.newFixedThreadPool(parallelCalls);
//
//        List<String> events = RedisPubSub.getEvents(consumerName + "_TOPIC_NAME");
//
//        for (String event : events) {
//            executorService.submit((new ConsumerFactory(consumerName)).getConsumerInstance());
//        }

    }

    public void executeForever() {
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                (new ConsumerFactory(consumerName)).getConsumerInstance().periodicallyExecute();
            }
        },0,(Integer) AppConfigFinder.get(consumerName + "_WAIT_FOR_UPDATE"), TimeUnit.SECONDS);

        RedisPubSub.addSubscriber(consumerName + "_TOPIC_NAME", new Subscriber(consumerName));
    }



}
