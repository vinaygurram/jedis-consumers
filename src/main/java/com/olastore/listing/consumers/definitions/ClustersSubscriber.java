package com.olastore.listing.consumers.definitions;

import com.olastore.listing.consumers.lib.Subscriber;
import com.olastore.listing.consumers.lib.SubscriberCategory;
import com.olastore.listing.consumers.lib.Event;
import com.olastore.listing.consumers.scheduler.SubscriberLauncher;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.olastore.listing.consumers.scheduler.UpdatesDemon;
import redis.clients.jedis.JedisPubSub;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by meetanshugupta on 07/10/15.
 */
public class ClustersSubscriber extends JedisPubSub implements Subscriber {

  UpdatesDemon updatesDemon;
  public static Logger logger = LoggerFactory.getLogger(ClustersSubscriber.class);

  public ClustersSubscriber (){
    updatesDemon = new UpdatesDemon();
    updatesDemon.runDemon();
  }

  @Override
  public SubscriberCategory getName() {
    return SubscriberCategory.CLUSTERS_SUBSCRIBER;
  }

  @Override
  public List<Event> getApplicableEvents() {
    return new ArrayList<Event>(){{
      add(Event.SELLER_STORE_ACTIVE);
      add(Event.SELLER_STORE_INACTIVE);
      add(Event.SELLER_ITEM_UPDATE);
      add(Event.SELLER_ITEM_DELETE);
      add(Event.SELLER_ITEM_ADD);
    }};
  }

  @Override
  public void onMessage(String channel,  String message){

    try {
      if(channel.contentEquals((String) SubscriberLauncher.redisConfigReader.readValue("redis_channel_name"))) {
        JSONObject messageObject = new JSONObject(message);
        System.out.println(messageObject);
        if(messageObject.has("message")){
          //this event is an inverntory event
          updatesDemon.updatedStores.get("product_change").add(messageObject.getString("store_id"));
        }else if(messageObject.has("collection_type")){
          //store update event
          if(messageObject.getString("state").contentEquals("active")){
            updatesDemon.updatedStores.get("active").add(messageObject.getString("store_id"));
          }else if(messageObject.getString("state").contentEquals("inactive")){
            updatesDemon.updatedStores.get("inactive").add(messageObject.getString("store_id"));
          }
        }
      }
    }catch (Exception e){
      logger.error("Exception {}",e);
    }

  }




}
