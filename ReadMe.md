##Consumers for Clusters##

This project aim to update the clusters on periodic basis. 


####Flow :: 
Clustering will take events from listing. Once events are successful listing will push the events into redis pub sub. Clustering will pick up
the events from redis pub sub. Catalog tree will be picked up from the listing by doing the aggregations.

Events expecting from the listing index :
1. inventory add/update/delete (shop item add/update/delete)
2. product status  from catalog team ( to make it undone)
3. store updates (only for store online & offline)
4. device ping  updating store status
5. device updates   updating store status
6. category path   will handle in the night;;

1. inventory add/update/delete  :: give me store ids; recalculate the product and sub cat coverage for clusters containing this stores
2. product status :: give me stores then clusters recalculate the product and sub cat coverage for clusters containing this stores
3. store updates (only addition of stores;) will automatically handle in the night;; moving forward :: rebuild the clusters and upsert;;
4. device ping (store status ) give me store ids (make clusters online/offline)
5. device update (store status) give me store ids (make clusters online/offline)
6. category path : only change in sub cat and product coverage



topic name :: cluster_store_update
topic_message structure ::

{
"type" : "online/offline/active/inactive/product_change",
"store_id" : "100034"
}

Total Product coverage and sub category coverage for a geo hash will get updated only when clusters are rebuild.


