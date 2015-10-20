package com.olastore.listing.consumers.definitions;

import java.util.Set;

/**
 * Created by gurramvinay on 10/21/15.
 */
public class Cluster {
  private String id;
  private Set<String> activeStores;
  private double rank;
  private String city_code;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public Set<String> getActiveStores() {
    return activeStores;
  }

  public void setActiveStores(Set<String> activeStores) {
    this.activeStores = activeStores;
  }

  public double getRank() {
    return rank;
  }

  public void setRank(double rank) {
    this.rank = rank;
  }

  public void removeStore(String storeId){
    if(this.activeStores.contains(storeId)){
      this.activeStores.remove(storeId);
    }
  }

  public String getCity_code() {
    return city_code;
  }

  public void setCity_code(String city_code) {
    this.city_code = city_code;
  }

  @Override
  public boolean equals(Object object){
    if(object instanceof Cluster){
      Cluster clusterObj = (Cluster) object;
      if(clusterObj.getId().contentEquals(id)) return true;
    }
    return false;
  }
}
