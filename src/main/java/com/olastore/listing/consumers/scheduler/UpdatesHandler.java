package com.olastore.listing.consumers.scheduler;

import com.olastore.listing.consumers.definitions.Cluster;
import com.olastore.listing.consumers.definitions.Store;
import com.olastore.listing.consumers.lib.EventMessage;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by gurramvinay on 8/20/15.
 */
public class UpdatesHandler {
  public ConcurrentHashMap<String,Set<EventMessage>> updatedStores = new ConcurrentHashMap<>();
  public static Logger logger = LoggerFactory.getLogger(UpdatesHandler.class);
  public static boolean isStoresInactive = false;

  public void clearStores(ConcurrentHashMap<String, Set<EventMessage>> map){
    //make empty hashsets
    System.out.println("clear stores happened");
    map.put("active", new HashSet<EventMessage>());
    map.put("inactive",new HashSet<EventMessage>());
    map.put("online",new HashSet<EventMessage>());
    map.put("offline",new HashSet<EventMessage>());
    map.put("productChange",new HashSet<EventMessage>());
  }

  public HashMap<String,Cluster> getClustersWithStores(HashMap<String,Store> stores){

    HashMap<String,Cluster> clusters = new HashMap<>();
    try {

      StringBuilder storeIdString = new StringBuilder();
      for(String storeId: stores.keySet()){
        storeIdString.append(',');
        storeIdString.append("\""+storeId+"\"");
      }
      logger.info("Store ids is "+ storeIdString);
      String query = "{\"size\" : 3000,\"query\":{\"terms\":{\"stores\":["+storeIdString.toString().substring(1)+"]}}}";
      JSONObject result = SubscriberLauncher.esClient.searchES((String)SubscriberLauncher.esConfigReader.readValue("clusters_index_name"),(String)SubscriberLauncher.esConfigReader.readValue("clusters_index_type"),query);
      result = result.getJSONObject("hits");
      JSONArray hitsArray = result.getJSONArray("hits");
      for(int i=0;i<hitsArray.length();i++){
        JSONObject thisStoreObject = hitsArray.getJSONObject(i);
        String id = thisStoreObject.getString("_id");
        Cluster cluster = new Cluster();
        cluster.setId(id);

        JSONArray activeStores = thisStoreObject.getJSONObject("_source").getJSONArray("stores");
        Set<String> newActiveStores = new HashSet<>();
        boolean isSetCityCode = false;
        if(isStoresInactive){
          for(int j=0;j<activeStores.length();j++){
            String activeStoreId = activeStores.getString(j);
            if(stores.containsKey(activeStoreId)){
              cluster.setCity_code(stores.get(activeStoreId).getCityCode());
              if(stores.get(activeStoreId).isActive()) newActiveStores.add(activeStoreId);
            }else {
              newActiveStores.add(activeStoreId);
            }
          }
        }else {
          for(int k=0;k<activeStores.length();k++){
            newActiveStores.add((String)activeStores.get(k));
            if(!isSetCityCode){
              if(stores.containsKey(activeStores.get(k))){
                cluster.setCity_code(stores.get(activeStores.get(k)).getCityCode());
                isSetCityCode = false;
              }

            }
          }
        }
        cluster.setActiveStores(newActiveStores);
        clusters.put(id,cluster);
      }
    }catch (Exception e){
      logger.error("Exception {}",e);
    }
    return clusters;
  }


  public void updateRankOfCluster(Cluster cluster){

    try {
      Set<String> productsSet = new HashSet<>();
      Set<String> stores  =  cluster.getActiveStores();
      if(stores.size()==0){
        cluster.setRank(0d);
        System.out.println("sdfds sdsf " + cluster.getId());
        return;
      }
      String storeIdString = "";
      for(String s: stores){
        storeIdString += "\""+s+"\",";
      }
      System.out.println("storesId string is "+storeIdString);
      storeIdString = storeIdString.substring(0,storeIdString.length()-1);

      String query = "{\"size\": 0,\"query\":{\"filtered\":{\"filter\":{\"bool\":{\"must\":[" +
          "{\"terms\":{\"store_details.id\":["+storeIdString+"]}}," +
          "{\"term\":{\"product_details.available\":true}}," +
          "{\"term\":{\"product_details.status\":\"current\"}}]}}}}," +
          "\"aggregations\":{\"unique_products\":{\"terms\":{\"field\":\"product_details.id\",\"size\":0}}}}";
      System.out.println(query);
      JSONObject result = SubscriberLauncher.esClient.searchES((String) SubscriberLauncher.esConfigReader.readValue("listing_index_name")+"_"+cluster.getCity_code(),
          (String) SubscriberLauncher.esConfigReader.readValue("listing_index_type"), query);
      System.out.println(result);
      JSONObject esResult = result.getJSONObject("aggregations");
      JSONArray uniqueProdBuckets = esResult.getJSONObject("unique_products").getJSONArray("buckets");
      for(int i=0;i<uniqueProdBuckets.length();i++){
        String productId = uniqueProdBuckets.getJSONObject(i).getString("key");
        productsSet.add(productId);
      }

      Set<String> intesection = new HashSet<String>(productsSet);
      intesection.retainAll(SubscriberLauncher.popularProductsSet);
      int popular_products_count = intesection.size();
      double rank = ((double) popular_products_count/(double)SubscriberLauncher.popularProductsSet.size());
      cluster.setRank(rank);

    }catch (Exception e){
      logger.error("Exception {}",e);
      cluster.setRank(0);
    }
  }

  public void runDemon () {

    clearStores(updatedStores);
    ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
      public void run() {
        try{
          isStoresInactive = false;

          HashMap<String, Set<EventMessage>> copyMap;
          StringBuilder bulkDoc = new StringBuilder();
          logger.info("Copy map is " + updatedStores);

          synchronized (updatedStores) {
            copyMap = new HashMap<String, Set<EventMessage>>(updatedStores);
            clearStores(updatedStores);
          }

          HashMap<String,Store> storesMap = new HashMap<String, Store>();


          //active stores
          //not doing anything for inactive to active


          //inactive stores
          for (EventMessage eventMessage : copyMap.get("inactive")) {
            isStoresInactive = true;
            Store store = new Store();
            store.setId(eventMessage.getStoreId());
            store.setIsActive(false);
            store.setCityCode(eventMessage.getCityCode());
            storesMap.put(eventMessage.getStoreId(),store);
          }

          //product coverage also updates rank
          for (EventMessage eventMessage : copyMap.get("productChange")) {
            if(!storesMap.containsKey(eventMessage.getStoreId())){
              Store store = new Store();
              store.setId(eventMessage.getStoreId());
              store.setIsActive(true);
              store.setCityCode(eventMessage.getCityCode());
              storesMap.put(eventMessage.getStoreId(),store);
            }
          }

          //update rank for the clusters
          if(!storesMap.isEmpty()){
            HashMap<String,Cluster> clusters = getClustersWithStores(storesMap);
            int count = 0;
            for(String key: clusters.keySet()){
              Cluster cluster = clusters.get(key);
              updateRankOfCluster(cluster);
              StringBuilder storesString = new StringBuilder();
              for(String activeStore : cluster.getActiveStores()){
                storesString.append(",");
                storesString.append("\"").append(activeStore).append("\"");
              }
              bulkDoc.append("{\"update\": {\"_id\" : \"" + key + "\",\"_type\" : \"" + SubscriberLauncher.esConfigReader.readValue("clusters_index_type") + "\", \"_index\" : \"" + SubscriberLauncher.esConfigReader.readValue("clusters_index_name") + "\"}}\n");
              if(cluster.getActiveStores().size()==0){
                bulkDoc.append("{\"doc\": {\"rank\":\"" + cluster.getRank() + "\", \"stores\" : []}}\n");
              }else {
                bulkDoc.append("{\"doc\": {\"rank\":\"" + cluster.getRank() + "\", \"stores\" : ["+storesString.toString().substring(1)+"]}}\n");
              }
              if(count>100){
                count =0;
                try {
                  if (!bulkDoc.toString().isEmpty()) {
                    logger.info("bulk doc is" + bulkDoc);
                    SubscriberLauncher.esClient.pushToESBulk("", "", bulkDoc.toString());
                    Thread.sleep(3000);
                    bulkDoc = new StringBuilder();
                  }
                } catch (Exception e) {
                  logger.error("Exception {}", e);
                }
              }
            }
            SubscriberLauncher.esClient.pushToESBulk("", "", bulkDoc.toString());
          }
        }catch (Exception e){
          e.printStackTrace();
        }
      }
    }, 0, 40, TimeUnit.SECONDS);
  }
}
