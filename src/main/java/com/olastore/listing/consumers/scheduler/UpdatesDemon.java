package com.olastore.listing.consumers.scheduler;

import com.olastore.listing.clustering.clients.ESClient;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
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
public class UpdatesDemon{
  public ConcurrentHashMap<String,Set<String>> updatedStores = new ConcurrentHashMap<String,Set<String>>();
  public static Logger logger = LoggerFactory.getLogger(UpdatesDemon.class);

  public void clearStores(ConcurrentHashMap<String,Set<String>> map){
    //make empty hashsets
    map.put("active", new HashSet<String>());
    map.put("inactive",new HashSet<String>());
    map.put("online",new HashSet<String>());
    map.put("offline",new HashSet<String>());
    map.put("productChange",new HashSet<String>());
  }

  public List<String> getClustersWithStoreId(String storeId){

    List<String> clusterIdList = new ArrayList<String>();
    try {
      logger.info("Store id is "+ storeId);
      String query = "{\"size\" : 1000,\"_source\":\"false\",\"query\":{\"term\":{\"stores.store_id\":\""+storeId+"\"}}}";
      JSONObject result = SubscriberLauncher.esClient.searchES((String)SubscriberLauncher.esConfigReader.readValue("clusters_index_name"),(String)SubscriberLauncher.esConfigReader.readValue("clusters_index_type"),query);
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


  public HashMap<String,Double> getCoverageOfCluster(String clusterId){

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
      JSONObject result = SubscriberLauncher.esClient.searchES((String) SubscriberLauncher.esConfigReader.readValue("listing_index_name"),
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

        ConcurrentHashMap<String,Set<String>> copyMap;
        StringBuilder bulkDoc = new StringBuilder();
        logger.info("Copy map is "+updatedStores);

        synchronized (updatedStores){
          copyMap = new ConcurrentHashMap<String, Set<String>>(updatedStores);
          clearStores(updatedStores);
        }

        //active stores
        for(String activeStoreID: copyMap.get("active")){
          List<String> clusterIds = getClustersWithStoreId(activeStoreID);
          for(String clusterId: clusterIds){
            bulkDoc.append("{\"update\": {\"_id\" : \""+clusterId+"\"}}\n");
            bulkDoc.append("{\"doc\": {\"state\":\"active\"}}\n");
          }
        }

        //inactive stores
        for(String inactiveStoreId: copyMap.get("inactive")){
          List<String> clusterIds = getClustersWithStoreId(inactiveStoreId);
          for(String clusterId: clusterIds){
            bulkDoc.append("{\"update\": {\"_id\" : \""+clusterId+"\"}}\n");
            bulkDoc.append("{\"doc\": {\"state\":\"inactive\"}}\n");
          }
        }

        //product coverage also updates rank
        for(String pchangeStoreId: copyMap.get("productChange")){
          List<String> clusterIds = getClustersWithStoreId(pchangeStoreId);
          for(String clusterId: clusterIds){
            HashMap<String,Double> coverageMap = getCoverageOfCluster(clusterId);
            bulkDoc.append("{\"update\": {\"_id\" : \""+clusterId+"\"}}\n");
            bulkDoc.append("{\"doc\": {\"rank\":\""+coverageMap.get("rank")+"\",\"product_count\":\""+coverageMap.get("product_cov")+"\",\"sub_cat_count\":\""+coverageMap.get("sub_cat_cov")+"\"}}\n");
          }
        }

        logger.info("bulk doc is"+ bulkDoc);

        try {
          if(!bulkDoc.toString().isEmpty()){
            SubscriberLauncher.esClient.pushToESBulk((String)SubscriberLauncher.esConfigReader.readValue("clusters_index_name"),
                (String)SubscriberLauncher.esConfigReader.readValue("clusters_index_type"),bulkDoc.toString());

            //String clusters_bulk_api = (String)RedisPubSub.yamlMap.get("clusters_bulk_api");
            String clusters_bulk_api = "";
            //TODO
            HttpClient httpClient = HttpClientBuilder.create().build();
            HttpPost httpPost = new HttpPost(clusters_bulk_api);
            httpPost.setEntity(new StringEntity(bulkDoc.toString()));
            HttpResponse httpResponse = httpClient.execute(httpPost);
            logger.info("status is "+httpResponse.getStatusLine().getStatusCode());
          }

        }catch (Exception e){
          logger.error("Exception {}",e);
        }
      }
    },0,10, TimeUnit.HOURS);
  }
}
