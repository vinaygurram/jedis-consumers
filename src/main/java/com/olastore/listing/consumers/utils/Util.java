package com.olastore.listing.consumers.utils;

import com.olastore.listing.clustering.utils.ConfigReader;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.security.krb5.Config;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by gurramvinay on 10/11/15.
 */
public class Util {

  public static Logger logger = LoggerFactory.getLogger(Util.class);

  public Set<String> initializePopularProductSet() {
    Set<String> productIdSet = new HashSet<>();
    FileReader fileReader = null;
    BufferedReader bufferedReader = null;
    try {
      File file = new File("tmpFile");
      FileUtils.copyInputStreamToFile(getClass().getClassLoader().getResourceAsStream("data/popular_products.csv"), file);
      fileReader = new FileReader(file);
      bufferedReader = new BufferedReader(fileReader);
      String line = bufferedReader.readLine();
      while ((line = bufferedReader.readLine()) != null) {
        productIdSet.add(line);
      }
      file.delete();
    } catch (Exception e) {
      logger.error("Exception happened!{}", e);
    } finally {
      try {
        if (fileReader != null) fileReader.close();
        if (bufferedReader != null) bufferedReader.close();
      } catch (Exception e) {
        logger.error("Exception happened!{}", e);
      }
    }
    return productIdSet;
  }
}
