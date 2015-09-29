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
            System.out.println("store id is "+ storeId);
            String clusterAPI = "http://localhost:9200/live_geo_clusters/_search";
            String query = "{\"size\" : 1000,\"_source\":\"false\",\"query\":{\"term\":{\"stores.store_id\":\""+storeId+"\"}}}";
            HttpClient httpClient = HttpClientBuilder.create().build();
            HttpPost httpPost = new HttpPost(clusterAPI);
            httpPost.setEntity(new StringEntity(query));
            HttpResponse httpResponse = httpClient.execute(httpPost);
            System.out.println("status is "+httpResponse.getStatusLine().getStatusCode());
            JSONObject result = new JSONObject(EntityUtils.toString(httpResponse.getEntity()));
            result = result.getJSONObject("hits");
            JSONArray hitsArray = result.getJSONArray("hits");
            for(int i=0;i<hitsArray.length();i++){
                JSONObject thisObject = hitsArray.getJSONObject(i);
                String clusterId = thisObject.getString("_id");
                clusterIdList.add(clusterId);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return clusterIdList;
    }


    public HashMap<String,Integer> getCoverageOfCluster(String clusterId){

        HashMap<String,Integer> coverageMap = new HashMap<String, Integer>();
        coverageMap.put("product_coverage", 0 );
        coverageMap.put("sub_cat_coverage", 0 );
        try {
            String ESAPI = "http://localhost:9200/listing/_search" ;
            String[] stores  = clusterId.split("-");
            String storeIdString = "";
            for(String s: stores){
                storeIdString += "\""+s+"\",";
            }
            storeIdString = storeIdString.substring(0,storeIdString.length()-1);
            String query= "{\"size\":0,\"query\":{\"terms\":{\"store.id\":["+storeIdString+"]}}," +
                    "\"aggregations\":{\"product_coverage\":{\"cardinality\":{\"field\":\"product.id\"}}," +
                    "\"sub_cat_coverage\":{\"cardinality\":{\"field\":\"product.sub_cat_id\"}}}}";

            query = "{\"size\":0,\"query\":{\"bool\":{\"must\":[" +
                    "{\"terms\":{\"store_details.id\":[\""+storeIdString+"\"]}}," +
                    "{\"term\":{\"product_details.available\":\"true\"}}," +
                    "{\"term\":{\"product_details.status\":\"current\"}}]}}," +
                    "\"aggregations\":{\"product_coverage\":{\"cardinality\":{\"field\":\"product_details.id\"}}," +
                    "\"sub_cat_coverage\":{\"cardinality\":{\"field\":\"product_details.sub_category_id\"}}}}";

            HttpClient httpClient = HttpClientBuilder.create().build();
            HttpPost httpPost = new HttpPost(ESAPI);
            httpPost.setEntity(new StringEntity(query));
            HttpResponse httpResponse = httpClient.execute(httpPost);
            JSONObject resultObject = new JSONObject(EntityUtils.toString(httpResponse.getEntity()));
            JSONObject aggrs = resultObject.getJSONObject("aggregations");
            int subCatCov = aggrs.getJSONObject("sub_cat_coverage").getInt("value");
            int productCov = aggrs.getJSONObject("product_coverage").getInt("value");
            coverageMap.put("sub_cat_cov",subCatCov);
            coverageMap.put("product_cov",productCov);
        }catch (Exception e){
            e.printStackTrace();
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
                System.out.println("copy map is"+updatedStores);

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

                //product coverage
                for(String pchangeStoreId: copyMap.get("productChange")){
                    List<String> clusterIds = getClustersWithStoreId(pchangeStoreId);
                    for(String clusterId: clusterIds){
                        HashMap<String,Integer> coverageMap = getCoverageOfCluster(clusterId);
                        bulkDoc.append("{\"update\": {\"_id\" : \""+clusterId+"\"}}\n");
                        bulkDoc.append("{\"doc\": {\"product_count\":\""+coverageMap.get("product_cov")+"\",\"sub_cat_count\":\""+coverageMap.get("sub_cat_cov")+"\"}}\n");
                    }
                }

                System.out.println("bulk doc is"+ bulkDoc);

                try {
                    if(!bulkDoc.toString().isEmpty()){

                        String ES_API = "http://localhost:9200/live_geo_clusters/geo_cluster/_bulk";
                        HttpClient httpClient = HttpClientBuilder.create().build();
                        HttpPost httpPost = new HttpPost(ES_API);
                        httpPost.setEntity(new StringEntity(bulkDoc.toString()));
                        HttpResponse httpResponse = httpClient.execute(httpPost);
                        System.out.println("status is "+httpResponse.getStatusLine().getStatusCode());
                    }

                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        },0,10, TimeUnit.SECONDS);
    }
}
