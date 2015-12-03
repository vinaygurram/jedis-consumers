## Consumers for Clusters

This project aim to update the clusters on periodic basis. 


### Story :: 
Clustering will take events from listing. Once events are successful listing will push the events into redis pub sub. Clustering will pick up
the events from redis pub sub.

Events expecting from the listing index :
topic name :: cluster_updates

From clusters POV, an event can be store_update or a product_update. Clusters consumer listens to the event and store
 them in hashmap by event type. Events are processed at 20 min interval by dumping the hashmap. Updates from the 
 events are processed and send to clusers in a bulk push, 100 documents at once.
 
 Please note that if a new store is added, the computation takes only at night. So if a store is onboarded during the
  day, Products from the store are not shown during that particular day. It can get orders using store serving logic. 
   From next day products from that store are shown.
   
### Installation
* Prerequisites : Running Redis Server, ElasticSearch, Java, Maven, Git
* clone the repo
* start the consume using 
```nohup java -jar cluster-consumer-1.0-jar-with-dependencies.jar dev >>logs/clusters.log &```

### Production Deployment
 * Make jar file with all the dependencies for PROD
 ``` mvn clean install```
 * Make jar file with all the dependencies for QA by changing the time from 20 minutes to 1 minute and then create 
 jar using 
 ``` mvn clean install```
 rename the jar to *target/cluster-consumer-1.0-jar-with-dependencies_qa.jar*
 * Push your changes to release branch. Add a tag if you like.
 * copy both the jar files to *$LISTING_HOME/bin/clusters*
 * Complete listing deployment on listing 01.
 * find the pid of the running consumer and not it down
 ```ps -eaf | grep '[c]luster-consumer' | gawk 'NR==1{print $2}'```
 *start  new clusters consumer using the following command
 ```
 cd /usr/share/ola/listing-service/bin/clusters/
 nohup java -jar cluster-consumer-1.0-jar-with-dependencies.jar prod >>/usr/share/ola/logs/listing-service/production.log &
 ```
 * Wait for 20 minutes and kill the old consumer. This is done so that there will be zero data loss.
 ```kill -9 $PID```
 * DONE

