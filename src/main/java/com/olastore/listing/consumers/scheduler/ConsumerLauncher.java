package com.olastore.listing.consumers.scheduler;

import com.olastore.listing.consumers.lib.ConsumerFactory;
import com.olastore.listing.consumers.utils.AppConfigFinder;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Created by meetanshugupta on 07/10/15.
 */
public class ConsumerLauncher {


    public void launchConsumer(String consumerName) {

        AppConfigFinder.setStage("development");

        EventsManager eventsManager = new EventsManager(consumerName);

        if ("FOREVER".toLowerCase().equals((String) AppConfigFinder.get(consumerName + "_EXECUTION_TYPE"))) {
            eventsManager.executeForever();
        } else {
            eventsManager.execute();
        }
    }

}
