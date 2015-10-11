package com.olastore.listing.consumers.lib;

/**
 * Created by gurramvinay on 10/12/15.
 */
public class EventMessage {
  private String cityCode;
  private String storeId;

  public String getCityCode() {
    return cityCode;
  }

  public void setCityCode(String cityCode) {
    this.cityCode = cityCode;
  }

  public String getStoreId() {
    return storeId;
  }

  public void setStoreId(String storeId) {
    this.storeId = storeId;
  }
}
