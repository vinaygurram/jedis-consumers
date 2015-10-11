import com.olastore.listing.clustering.utils.ConfigReader;
import com.olastore.listing.consumers.scheduler.SubscriberLauncher;
import com.olastore.listing.consumers.utils.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * Created by gurramvinay on 10/11/15.
 */
public class App {
  public static Logger logger = LoggerFactory.getLogger(App.class);


  public static void main(String[] args) {

    try {
      logger.info("welcome");
      //read env and city
      if(args.length<2 || args[0].isEmpty() ||args[1]==null || args[1].isEmpty()){
        logger.error("First parameter env and second parameter city must not be null or empty");
        return;
      }
      String env = args[0];
      String city = args[1];

      //read config
      ConfigReader esConfigReader = new ConfigReader("config/es.yaml");
      ConfigReader redisConfigReader = new ConfigReader("config/redis.yaml");
      Set<String> popularProductsSet = new Util().initializePopularProductSet();

      SubscriberLauncher subscriberLauncher = new SubscriberLauncher(esConfigReader,redisConfigReader,popularProductsSet,city,env);
      subscriberLauncher.startListening();

    }catch (Exception e){
      logger.error("Exception {}",e);
    }
  }
}
