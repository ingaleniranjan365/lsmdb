package com.lsmdb;

import com.lsmdb.entity.MemTableWrapper;
import com.lsmdb.service.FileIOService;
import com.lsmdb.service.LSMService;
import com.lsmdb.service.MergeService;
import com.lsmdb.service.SegmentService;

public class LSM {
        public static void main(String[] args) {
                System.out.println("hello world!");
        }

        public static LSMService getLsmService() {
                final var fileIOService = new FileIOService();
                final var stateLoader = new StateLoader(fileIOService);
                final var segmentConfig = stateLoader.getSegmentConfig();
                final var indices = stateLoader.getIndices();
                final var mergeService = new MergeService(fileIOService);
                final var segmentService = new SegmentService(segmentConfig, fileIOService);
                final var memTable = stateLoader.getMemTableFromWAL();
                final var memTableWrapper = new MemTableWrapper(memTable, indices, fileIOService, segmentService);
                return new LSMService(memTableWrapper, indices, fileIOService, segmentService, mergeService);
        }


}
