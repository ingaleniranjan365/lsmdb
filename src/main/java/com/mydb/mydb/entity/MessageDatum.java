package com.mydb.mydb.entity;

import java.io.Serializable;

public class MessageDatum implements Serializable {

  public String measureName;
  public String measureCode;
  public String measureUnit;
  public String measureValue;
  public String measureValueDescription;
  public String measureType;
  public double componentReading;
}
