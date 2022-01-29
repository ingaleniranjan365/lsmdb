package com.mydb.mydb;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SegmentConfig {
  private String basePath;
  private long count;
}
