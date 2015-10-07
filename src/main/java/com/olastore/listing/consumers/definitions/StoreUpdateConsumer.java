package com.olastore.listing.consumers.definitions;

import com.olastore.listing.consumers.lib.Consumer;
import com.olastore.listing.consumers.lib.ConsumerCategory;
import com.olastore.listing.consumers.lib.Event;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by meetanshugupta on 07/10/15.
 */
public class StoreUpdateConsumer implements Consumer {

    @Override
    public ConsumerCategory getName() {
        return ConsumerCategory.STORE_STATUS_UPDATE_CONSUMER;
    }

    @Override
    public List<Event> getApplicableEvents() {
        return new ArrayList<Event>(){{
            add(Event.SELLER_STORE_ACTIVE);
            add(Event.SELLER_STORE_INACTIVE);
        }};
    }

    @Override
    public String call() {

        return "SUCCESS";
    }


}
