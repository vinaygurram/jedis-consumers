package redis;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPubSub;

public class Subscriber extends JedisPubSub {

    private Logger logger = LoggerFactory.getLogger(Subscriber.class);
    public UpdatesDemon updatesDemon = new UpdatesDemon();

    Subscriber(){
        updatesDemon.runDemon();
    }

    @Override
    public void onMessage(String channel,  String message){

        try {
            if(channel.contentEquals("cluster_store_update")) {
                JSONObject messageObject = new JSONObject(message);

                if(messageObject.has("type")){
                    String type= messageObject.getString("type");
                    if(type.contentEquals("online")){
                        // ignore the online and offline stores as of now;;
                    }else  if(type.contentEquals("offline")){
                        // ignore the online and offline stores as of now;;
                    }else if(type.contentEquals("active")){
                        updatesDemon.updatedStores.get("active").add(messageObject.getString("store_id"));
                    }else if(type.contentEquals("inactive")){
                        updatesDemon.updatedStores.get("inactive").add(messageObject.getString("store_id"));
                    }else if(type.contentEquals("product_change")){
                        updatesDemon.updatedStores.get("productChange").add(messageObject.getString("store_id"));
                    }
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }

    }
    @Override
        public void onPMessage(String pattern, String channel, String message) {
        }
    @Override
        public void onSubscribe(String channel, int subscribedChannels) {
    }
    @Override
        public void onUnsubscribe(String channel, int subscribedChannels) {
    }
    @Override
        public void onPUnsubscribe(String pattern, int subscribedChannels) {
    }
    @Override
        public void onPSubscribe(String pattern, int subscribedChannels) {
    }



    public static void main(String[] args) {
    }

}
