package com.olastore.listing.consumers.scheduler;

import com.olastore.listing.consumers.utils.AppConfigFinder;

/**
 * Created by meetanshugupta on 07/10/15.
 */
public class ConsumerLauncher {


    public void launchConsumer(String consumerName) {

        AppConfigFinder.setStage("development");

        EventsManager eventsManager = new EventsManager(consumerName);
        eventsManager.execute();

    }

}
