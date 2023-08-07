package com.lsmdb.service;

import com.lsmdb.SegmentConfig;
import com.lsmdb.entity.Segment;

public class SegmentService {

        private final SegmentConfig segmentConfig;
        private final FileIOService fileIOService;
        private final String configPath;

        public SegmentService(final SegmentConfig segmentConfig, FileIOService fileIOService,
                              String configPath) {
                this.segmentConfig = segmentConfig;
                this.fileIOService = fileIOService;
                this.configPath = configPath;
        }

        private SegmentConfig getCurrentSegmentConfig() {
                return new SegmentConfig(segmentConfig.getSegmentsPath(), segmentConfig.getCount());
        }

        public Segment getNewSegment() {
                segmentConfig.setCount(segmentConfig.getCount() + 1);
                var newSegmentName = getSegmentName(segmentConfig.getCount());
                var newSegmentPath = getPathForSegment(newSegmentName);
                var newIndexName = getIndexName(segmentConfig.getCount());
                var newIndexPath = getPathForIndex(newIndexName);
                fileIOService.persistConfig(configPath, getCurrentSegmentConfig());
                return new Segment(
                        newSegmentName,
                        newSegmentPath,
                        newIndexName,
                        newIndexPath
                );
        }

        private String getSegmentName(long i) {
                return String.format("segment-%d", i);
        }

        private String getIndexName(long i) {
                return String.format("index-%d", i);
        }

        public String getPathForSegment(String segmentName) {
                return segmentConfig.getSegmentsPath() + "/" + segmentName;
        }

        public String getPathForIndex(String indexName) {
                return segmentConfig.getSegmentsPath() + "/indices/" + indexName;
        }

}
