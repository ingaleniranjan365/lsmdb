package com.mydb.mydb.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Segment {
  private String segmentName;
  private String segmentPath;
  private String backupName;
  private String backupPath;
}
