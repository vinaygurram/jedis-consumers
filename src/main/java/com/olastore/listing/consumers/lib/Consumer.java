package com.olastore.listing.consumers.lib;

import java.util.List;
import java.util.concurrent.Callable;

/**
 * Created by meetanshugupta on 07/10/15.
 */
public interface Consumer extends Callable<String> {

    public ConsumerCategory getName();

    public List<Event> getApplicableEvents();

    @Override
    public String call();
}
