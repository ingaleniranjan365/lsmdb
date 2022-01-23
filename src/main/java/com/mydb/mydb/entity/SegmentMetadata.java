package com.mydb.mydb.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SegmentMetadata implements Serializable {

  private static final long serialVersionUID = 5388380270261334689L;

  private int offset;
  private int size;
}
