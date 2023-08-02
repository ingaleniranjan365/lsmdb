package com.lsmdb.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serial;
import java.io.Serializable;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SegmentIndex implements Serializable {

  @Serial
  private static final long serialVersionUID = 5388380270261334686L;

  private Segment segment;

  private Map<String, SegmentMetadata> segmentIndex;
}
