package com.lsmdb.entity.merge;

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
  private String ids;
  private int index;

  public static Comparator<HeapElement> getHeapElementComparator() {
    return (e1, e2) -> {
      int comparison = e1.getIds().compareTo(e2.getIds());
      if (comparison == 0) {
        return String.valueOf(e1.getIndex()).compareTo(String.valueOf(e2.getIndex()));
      }
      return comparison;
    };
  }

  public static boolean isIdPresentInList(final List<HeapElement> elements, final String id) {
    return elements.stream().anyMatch(e -> e.getIds().equals(id));
  }
}
