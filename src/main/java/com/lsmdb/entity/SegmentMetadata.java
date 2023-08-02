package com.lsmdb.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serial;
import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SegmentMetadata implements Serializable {

  @Serial
  private static final long serialVersionUID = 5388380270261334689L;

  private long offset;
  private long size;
}
