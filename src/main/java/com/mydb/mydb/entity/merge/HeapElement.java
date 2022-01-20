package com.mydb.mydb.entity.merge;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.Comparator;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class HeapElement {
  private String probeId;
  private int index;

  public static Comparator<HeapElement> getHeapElementComparator() {
    return (e1, e2) -> {
      int probeComparison = e1.getProbeId().compareTo(e2.getProbeId());
      if (probeComparison == 0) {
        return String.valueOf(e1.getIndex()).compareTo(String.valueOf(e2.getIndex()));
      }
      return probeComparison;
    };
  }

  public static boolean isProbeIdPresentInList(final List<HeapElement> elements, final String probeId) {
    return elements.stream().anyMatch(e -> e.getProbeId().equals(probeId));
  }
}
