package com.mydb.mydb.entity;

import java.io.Serial;
import java.io.Serializable;

public class MessageDatum implements Serializable {

  @Serial
  private static final long serialVersionUID = 5388380270261334691L;

  public String measureName;
  public String measureCode;
  public String measureUnit;
  public String measureValue;
  public String measureValueDescription;
  public String measureType;
  public double componentReading;
}
