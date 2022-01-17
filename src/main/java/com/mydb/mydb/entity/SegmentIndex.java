package com.mydb.mydb.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Collections;
import java.util.Enumeration;
import java.util.Map;

@Data
@AllArgsConstructor
public class SegmentIndex {

  private String segmentName;

  private Map<String, SegmentMetadata> index;
}
