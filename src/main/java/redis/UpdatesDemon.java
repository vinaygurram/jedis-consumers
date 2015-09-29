package redis;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;

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
      RedisPubSub.logger.info("Store id is "+ storeId);
      String clusterAPI = (String)RedisPubSub.yamlMap.get("clusters_serach_api");
      String query = "{\"size\" : 1000,\"_source\":\"false\",\"query\":{\"term\":{\"stores.store_id\":\""+storeId+"\"}}}";
      HttpClient httpClient = HttpClientBuilder.create().build();
      HttpPost httpPost = new HttpPost(clusterAPI);
      httpPost.setEntity(new StringEntity(query));
      HttpResponse httpResponse = httpClient.execute(httpPost);
      RedisPubSub.logger.info("Response from clusters serach is "+ httpResponse.getStatusLine());
      JSONObject result = new JSONObject(EntityUtils.toString(httpResponse.getEntity()));
      result = result.getJSONObject("hits");
      JSONArray hitsArray = result.getJSONArray("hits");
      for(int i=0;i<hitsArray.length();i++){
        JSONObject thisObject = hitsArray.getJSONObject(i);
        String clusterId = thisObject.getString("_id");
        clusterIdList.add(clusterId);
      }
    }catch (Exception e){
      RedisPubSub.logger.error(e.getMessage());
    }
    return clusterIdList;
  }


  public HashMap<String,Double> getCoverageOfCluster(String clusterId){

    HashMap<String,Double> coverageMap = new HashMap<String, Double>();
    coverageMap.put("product_coverage", 0d );
    coverageMap.put("sub_cat_coverage", 0d );
    coverageMap.put("rank",0d);

    try {
      Set<String> productsSet = new HashSet<>();
      String listing_search_api =(String)RedisPubSub.yamlMap.get("listing_search_api") ;
      String[] stores  = clusterId.split("-");
      String storeIdString = "";
      for(String s: stores){
        storeIdString += "\""+s+"\",";
      }
      storeIdString = storeIdString.substring(0,storeIdString.length()-1);
//      String query= "{\"size\":0,\"query\":{\"terms\":{\"store.id\":["+storeIdString+"]}}," +
//          "\"aggregations\":{\"product_coverage\":{\"cardinality\":{\"field\":\"product.id\"}}," +
//          "\"sub_cat_coverage\":{\"cardinality\":{\"field\":\"product.sub_cat_id\"}}}}";
//
//      query = "{\"size\":0,\"query\":{\"bool\":{\"must\":[" +
//          "{\"terms\":{\"store_details.id\":[\""+storeIdString+"\"]}}," +
//          "{\"term\":{\"product_details.available\":\"true\"}}," +
//          "{\"term\":{\"product_details.status\":\"current\"}}]}}," +
//          "\"aggregations\":{\"product_coverage\":{\"cardinality\":{\"field\":\"product_details.id\"}}," +
//          "\"sub_cat_coverage\":{\"cardinality\":{\"field\":\"product_details.sub_category_id\"}}}}";
      String query = "{\"size\": 0,\"query\":{\"filtered\":{\"filter\":{\"bool\":{\"must\":[" +
          "{\"terms\":{\"store_details.id\":["+storeIdString+"]}}," +
          "{\"term\":{\"product_details.available\":true}}," +
          "{\"term\":{\"product_details.status\":\"current\"}}]}}}}," +
          "\"aggregations\":{\"unique_products\":{\"terms\":{\"field\":\"product_details.id\",\"size\":0}}," +
          "\"sub_cat_count\":{\"cardinality\":{\"field\":\"product_details.sub_category_id\"}}}}";

      HttpClient httpClient = HttpClientBuilder.create().build();
      HttpPost httpPost = new HttpPost(listing_search_api);
      httpPost.setEntity(new StringEntity(query));
      HttpResponse httpResponse = httpClient.execute(httpPost);
      JSONObject result = new JSONObject(EntityUtils.toString(httpResponse.getEntity()));
      JSONObject esResult = result.getJSONObject("aggregations");
      JSONArray uniqueProdBuckets = esResult.getJSONObject("unique_products").getJSONArray("buckets");
      for(int i=0;i<uniqueProdBuckets.length();i++){
        String productId = uniqueProdBuckets.getJSONObject(i).getString("key");
        productsSet.add(productId);
      }
      int subCatCount = esResult.getJSONObject("sub_cat_count").getInt("value");
      Set<String> intesection = new HashSet<String>(productsSet);
      intesection.retainAll(RedisPubSub.popularProductsSet);
      int popular_products_count = intesection.size();
      //compute rank
      double rank = ((double) popular_products_count/(double)RedisPubSub.popularProductsSet.size());
      coverageMap.put("sub_cat_cov",(double)subCatCount);
      coverageMap.put("product_cov", (double) productsSet.size());
      coverageMap.put("rank",rank);
    }catch (Exception e){
      RedisPubSub.logger.error(e.getMessage());
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
        RedisPubSub.logger.info("Copy map is "+updatedStores);

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

        RedisPubSub.logger.info("bulk doc is"+ bulkDoc);

        try {
          if(!bulkDoc.toString().isEmpty()){

            String clusters_bulk_api = (String)RedisPubSub.yamlMap.get("clusters_bulk_api");
            HttpClient httpClient = HttpClientBuilder.create().build();
            HttpPost httpPost = new HttpPost(clusters_bulk_api);
            httpPost.setEntity(new StringEntity(bulkDoc.toString()));
            HttpResponse httpResponse = httpClient.execute(httpPost);
            RedisPubSub.logger.info("status is "+httpResponse.getStatusLine().getStatusCode());
          }

        }catch (Exception e){
          RedisPubSub.logger.error(e.getMessage());
        }
      }
    },0,10, TimeUnit.SECONDS);
  }
}
