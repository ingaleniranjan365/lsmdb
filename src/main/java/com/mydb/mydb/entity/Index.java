package com.mydb.mydb.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.concurrent.ConcurrentLinkedDeque;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class Index implements Serializable {

  private static final long serialVersionUID = 5388380270261334699L;

  private String backUpName;
  private ConcurrentLinkedDeque<SegmentIndex> indices;
}
