package com.olastore.listing.consumers.scheduler;

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

  public void clearStores(ConcurrentHashMap<String, Set<EventMessage>> map){
    //make empty hashsets
    System.out.println("clear stores happened");
    map.put("active", new HashSet<EventMessage>());
    map.put("inactive",new HashSet<EventMessage>());
    map.put("online",new HashSet<EventMessage>());
    map.put("offline",new HashSet<EventMessage>());
    map.put("productChange",new HashSet<EventMessage>());
  }

  public List<String> getClustersWithStoreId(String storeId,String city_code){

    List<String> clusterIdList = new ArrayList<String>();
    try {
      logger.info("Store id is "+ storeId);
      String query = "{\"size\" : 1000,\"_source\":\"false\",\"query\":{\"term\":{\"stores.store_id\":\""+storeId+"\"}}}";
      JSONObject result = SubscriberLauncher.esClient.searchES((String)SubscriberLauncher.esConfigReader.readValue("clusters_index_name")+"_"+city_code,(String)SubscriberLauncher.esConfigReader.readValue("clusters_index_type"),query);
      result = result.getJSONObject("hits");
      JSONArray hitsArray = result.getJSONArray("hits");
      for(int i=0;i<hitsArray.length();i++){
        JSONObject thisObject = hitsArray.getJSONObject(i);
        String clusterId = thisObject.getString("_id");
        clusterIdList.add(clusterId);
      }
    }catch (Exception e){
      logger.error("Exception {}",e);
    }
    return clusterIdList;
  }


  public HashMap<String,Double> getCoverageOfCluster(String clusterId,String city_code){

    HashMap<String,Double> coverageMap = new HashMap<String, Double>();
    coverageMap.put("rank",0d);

    try {
      Set<String> productsSet = new HashSet<>();
      String[] stores  = clusterId.split("-");
      String storeIdString = "";
      for(String s: stores){
        storeIdString += "\""+s+"\",";
      }
      storeIdString = storeIdString.substring(0,storeIdString.length()-1);

      String query = "{\"size\": 0,\"query\":{\"filtered\":{\"filter\":{\"bool\":{\"must\":[" +
          "{\"terms\":{\"store_details.id\":["+storeIdString+"]}}," +
          "{\"term\":{\"product_details.available\":true}}," +
          "{\"term\":{\"product_details.status\":\"current\"}}]}}}}," +
          "\"aggregations\":{\"unique_products\":{\"terms\":{\"field\":\"product_details.id\",\"size\":0}}}}}}";
      JSONObject result = SubscriberLauncher.esClient.searchES((String) SubscriberLauncher.esConfigReader.readValue("listing_index_name")+"_"+city_code,
          (String) SubscriberLauncher.esConfigReader.readValue("listing_index_type"), query);
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
      coverageMap.put("rank",rank);
    }catch (Exception e){
      logger.error("Exception {}",e);
    }
    return coverageMap;
  }

  public void runDemon () {

    clearStores(updatedStores);
    ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
      public void run() {

        ConcurrentHashMap<String,Set<EventMessage>> copyMap;
        StringBuilder bulkDoc = new StringBuilder();
        logger.info("Copy map is "+updatedStores);

        synchronized (updatedStores){
          copyMap = new ConcurrentHashMap<String, Set<EventMessage>>(updatedStores);
          clearStores(updatedStores);
        }

        //active stores
        for(EventMessage eventMessage: copyMap.get("active")){
          List<String> clusterIds = getClustersWithStoreId(eventMessage.getStoreId(),eventMessage.getCityCode());
          for(String clusterId: clusterIds){
            bulkDoc.append("{\"update\": {\"_id\" : \""+clusterId+"\",\"_type\" : \""+SubscriberLauncher.esConfigReader.readValue("clusters_index_type")+"\", \"_index\" : \""+SubscriberLauncher.esConfigReader.readValue("clusters_index_name")+"_"+eventMessage.getCityCode()+"\"}}\n");
            bulkDoc.append("{\"doc\": {\"status\":\"active\"}}\n");
          }
        }

        //inactive stores
        for(EventMessage eventMessage: copyMap.get("inactive")){
          List<String> clusterIds = getClustersWithStoreId(eventMessage.getStoreId(),eventMessage.getCityCode());
          for(String clusterId: clusterIds){
            bulkDoc.append("{\"update\": {\"_id\" : \""+clusterId+"\",\"_type\" : \""+SubscriberLauncher.esConfigReader.readValue("clusters_index_type")+"\", \"_index\" : \""+SubscriberLauncher.esConfigReader.readValue("clusters_index_name")+"_"+eventMessage.getCityCode()+"\"}}\n");
            bulkDoc.append("{\"doc\": {\"status\":\"inactive\"}}\n");
          }
        }

        //product coverage also updates rank
        for(EventMessage eventMessage: copyMap.get("productChange")){
          List<String> clusterIds = getClustersWithStoreId(eventMessage.getStoreId(),eventMessage.getCityCode());
          for(String clusterId: clusterIds){
            HashMap<String,Double> coverageMap = getCoverageOfCluster(clusterId,eventMessage.getCityCode());
            bulkDoc.append("{\"update\": {\"_id\" : \""+clusterId+"\",\"_type\" : \""+SubscriberLauncher.esConfigReader.readValue("clusters_index_type")+"\", \"_index\" : \""+SubscriberLauncher.esConfigReader.readValue("clusters_index_name")+"_"+eventMessage.getCityCode()+"\"}}\n");
            bulkDoc.append("{\"doc\": {\"rank\":\""+coverageMap.get("rank")+"\"}}\n");
          }
        }

        logger.info("bulk doc is"+ bulkDoc);

        try {
          if(!bulkDoc.toString().isEmpty()){
            SubscriberLauncher.esClient.pushToESBulk("","",bulkDoc.toString());
          }

        }catch (Exception e){
          logger.error("Exception {}",e);
        }
      }
    },0,10, TimeUnit.SECONDS);
  }
}
