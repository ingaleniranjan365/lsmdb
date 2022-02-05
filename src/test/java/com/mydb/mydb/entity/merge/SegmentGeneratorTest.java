package com.mydb.mydb.entity.merge;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;

class SegmentGeneratorTest {


  @Test
  void mapRemoveTest() {
    var map = new LinkedHashMap<String, String>();
    map.put("b", "b");
    map.put("a", "a");
    map.put("c", "c");

    assert map.size() == 3;

    map.keySet().stream().toList().subList(0, 2).stream().sorted().forEach(map::remove);

    assert map.size() == 1;
  }

}
