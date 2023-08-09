package com.lsmdb;

import com.lsmdb.entity.MemTableWrapper;
import com.lsmdb.service.FileIOService;
import com.lsmdb.service.LSMService;
import com.lsmdb.service.MergeService;
import com.lsmdb.service.SegmentService;

public class LSM {

        private static final String PATH_TO_HOME = System.getProperty("user.home");
        private static final String DB_DIR = PATH_TO_HOME + "/data";
        private static final String WAL_PATH = DB_DIR + "/wal";
        private static final String IN_MEMORY_RECORDS_CNT_PATH = DB_DIR + "/imrc";
        private static final Integer BUFFER_SIZE = 1000;
        private static final String SEGMENTS_PATH = PATH_TO_HOME + "/data/segments";
        private static final String CONFIG_PATH = PATH_TO_HOME + "/data/segmentState.json";
        private static final int IN_MEMORY_RECORD_CNT_SOFT_LIMIT = 10000;
        private static final int IN_MEMORY_RECORD_CNT_HARD_LIMIT = 50000;

        public static LSMService getLsmService() {
                final var fileIOService = new FileIOService(WAL_PATH, BUFFER_SIZE, IN_MEMORY_RECORDS_CNT_PATH,
                        IN_MEMORY_RECORD_CNT_HARD_LIMIT);
                final var stateLoader = new StateLoader(fileIOService, SEGMENTS_PATH, CONFIG_PATH, WAL_PATH);
                final var segmentConfig = stateLoader.getSegmentConfig();
                final var indices = stateLoader.getIndices();
                final var mergeService = new MergeService(fileIOService);
                final var segmentService = new SegmentService(segmentConfig, fileIOService, CONFIG_PATH);
                final var memTable = stateLoader.getMemTableFromWAL();
                final var memTableWrapper = new MemTableWrapper(memTable, indices, fileIOService, segmentService,
                        IN_MEMORY_RECORD_CNT_SOFT_LIMIT, IN_MEMORY_RECORD_CNT_HARD_LIMIT);
                return new LSMService(memTableWrapper, indices, fileIOService, segmentService, mergeService);
        }


}
