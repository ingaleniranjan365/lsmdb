package com.lsmdb.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serial;
import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Segment implements Serializable {
  @Serial
  private static final long serialVersionUID = 5388389970261334690L;

  private String segmentName;
  private String segmentPath;
  private String backupName;
  private String backupPath;
}
