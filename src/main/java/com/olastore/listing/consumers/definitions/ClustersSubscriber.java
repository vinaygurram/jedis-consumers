package com.olastore.listing.consumers.definitions;

import com.olastore.listing.consumers.lib.EventMessage;
import com.olastore.listing.consumers.lib.Subscriber;
import com.olastore.listing.consumers.lib.SubscriberCategory;
import com.olastore.listing.consumers.lib.Event;
import com.olastore.listing.consumers.scheduler.SubscriberLauncher;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.olastore.listing.consumers.scheduler.UpdatesHandler;
import redis.clients.jedis.JedisPubSub;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by meetanshugupta on 07/10/15.
 */
public class ClustersSubscriber extends JedisPubSub implements Subscriber {

  UpdatesHandler updatesHandler = new UpdatesHandler();
  public static Logger logger = LoggerFactory.getLogger(ClustersSubscriber.class);

  public ClustersSubscriber (){
    updatesHandler.runDemon();
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
          EventMessage eventMessage = new EventMessage();
          String city_code = messageObject.getString("city_code");
          city_code = city_code.toLowerCase();
          eventMessage.setCityCode(city_code);
          eventMessage.setStoreId(messageObject.getString("store_id"));
          String cityCode = eventMessage.getCityCode().toLowerCase();
          if(cityCode.contentEquals("vpm")){
            updatesHandler.updatedStores.get("productChange").add(eventMessage);
          }
        }else if(messageObject.has("collection_type")){
          //store update event
          EventMessage eventMessage = new EventMessage();
          eventMessage.setStoreId(messageObject.getString("store_id"));
          String city_code = messageObject.getString("city_code");
          city_code = city_code.toLowerCase();
          eventMessage.setCityCode(city_code);
          if(city_code.contentEquals("vpm")){
            if(messageObject.getString("state").contentEquals("active")){
              updatesHandler.updatedStores.get("active").add(eventMessage);
            }else if(messageObject.getString("state").contentEquals("inactive")){
              updatesHandler.updatedStores.get("inactive").add(eventMessage);
            }
          }

        }
      }
    }catch (Exception e){
      logger.error("Exception {}",e);
    }

  }




}
