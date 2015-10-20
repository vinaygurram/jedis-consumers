package com.olastore.listing.consumers.definitions;

/**
 * Created by gurramvinay on 10/21/15.
 */
public class Store {
  private String id;
  private boolean isActive;
  private String cityCode;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public boolean isActive() {
    return isActive;
  }

  public void setIsActive(boolean isActive) {
    this.isActive = isActive;
  }

  public String getCityCode() {
    return cityCode;
  }

  public void setCityCode(String cityCode) {
    this.cityCode = cityCode;
  }

  @Override
  public boolean equals(Object obj){
    if(obj instanceof Store){
      Store objStore = (Store) obj;
      if(objStore.getId().contentEquals(id)) return true;
    }
    return false;
  }
}
