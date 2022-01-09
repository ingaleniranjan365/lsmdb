package com.mydb.mydb.entity;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class Payload implements Serializable {

  private String probeId;
  private String eventId;
  private String messageType;
  private long eventReceivedTime;
  private long eventTransmissionTime;
  private List<MessageDatum> messageData;

}
