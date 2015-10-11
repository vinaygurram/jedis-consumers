package com.olastore.listing.consumers.lib;

import com.olastore.listing.consumers.definitions.ClustersSubscriber;

/**
 * Created by meetanshugupta on 07/10/15.
 */
public class SubscriberFactory {


  public SubscriberFactory() {
  }

  public Subscriber getSubscriberInstance(String name) {
    return new ClustersSubscriber();
  }
}
