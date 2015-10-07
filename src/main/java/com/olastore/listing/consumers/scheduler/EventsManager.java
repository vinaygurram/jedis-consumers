package com.olastore.listing.consumers.scheduler;

import com.olastore.listing.consumers.lib.ConsumerFactory;
import com.olastore.listing.consumers.utils.AppConfigFinder;
import redis.RedisPubSub;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Created by meetanshugupta on 07/10/15.
 */
public class EventsManager {

    private String consumerName;
    private int parallelCalls;

    public EventsManager(String consumerName){
        this.consumerName = consumerName;
        this.parallelCalls = (Integer) AppConfigFinder.get(consumerName + "_PARALLEL_CALLS");

    }

    public void execute() {

        ExecutorService executorService = Executors.newFixedThreadPool(parallelCalls);

        List<String> events = RedisPubSub.getEvents(consumerName + "_TOPIC_NAME");

        for (String event : events) {
            executorService.submit((new ConsumerFactory(consumerName)).getConsumerInstance());
        }

    }

}
