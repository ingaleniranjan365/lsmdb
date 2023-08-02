package com.lsmdb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lsmdb.entity.SegmentIndex;
import com.lsmdb.service.FileIOService;
import io.vertx.core.buffer.Buffer;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.time.Instant;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;

public class StateLoader {

        // TODO: Remove duplication of conf
        public static final String PATH_TO_HOME = System.getProperty("user.home");
        public static final String CONFIG_PATH = PATH_TO_HOME + "/data/segmentState.json";
        public static final String DEFAULT_BASE_PATH = PATH_TO_HOME + "/data/segments";
        // TODO: Remove this mapper and use common mapper
        public static final ObjectMapper mapper = new ObjectMapper();

        private final FileIOService fileIOService;

        public StateLoader(final FileIOService fileIOService) {
                this.fileIOService = fileIOService;
        }

        public SegmentConfig getSegmentConfig() {
                var segmentConfig = fileIOService.getSegmentConfig(CONFIG_PATH);
                if (segmentConfig.isPresent()) {
                        var config = segmentConfig.get();
                        config.setBasePath(DEFAULT_BASE_PATH);
                        return config;
                }
                return new SegmentConfig(DEFAULT_BASE_PATH, -1);
        }

        public Deque<SegmentIndex> getIndices() {
                var segmentConfig = fileIOService.getSegmentConfig(CONFIG_PATH);
                var indices = new ConcurrentLinkedDeque<SegmentIndex>();
                if (segmentConfig.isPresent()) {
                        var counter = segmentConfig.get().getCount();
                        while (counter >= 0) {
                                var index = fileIOService.getIndex(DEFAULT_BASE_PATH + "/indices/backup-" + counter);
                                index.ifPresent(indices::addLast);
                                counter--;
                        }
                }
                return indices;
        }

        public Map<String, ImmutablePair<Instant, Buffer>> getMemTableFromWAL() {
                // TODO : this method needs impl, dependant on impl of WAL which isn't implemented at the moment
                return new ConcurrentSkipListMap<>();
        }

}
