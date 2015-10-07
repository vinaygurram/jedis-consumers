package com.olastore.listing.consumers.lib;

import java.util.List;
import java.util.concurrent.Callable;

/**
 * Created by meetanshugupta on 07/10/15.
 */
public interface Consumer {

    public ConsumerCategory getName();

    public List<Event> getApplicableEvents();

    public String processEvent(String event);

    public String periodicallyExecute();
}
