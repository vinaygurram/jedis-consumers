package com.olastore.listing.consumers.lib;

import com.olastore.listing.consumers.definitions.StoreUpdateSubscribingConsumer;

/**
 * Created by meetanshugupta on 07/10/15.
 */
public class ConsumerFactory {

    String consumerName;

    public ConsumerFactory(String name) {
        this.consumerName = name;

    }

    public SubscribingConsumer getConsumerInstance() {

        return new StoreUpdateSubscribingConsumer();
    }
}
