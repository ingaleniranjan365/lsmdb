package com.lsmdb.service;

import com.lsmdb.entity.MemTableWrapper;
import com.lsmdb.entity.SegmentIndex;
import com.lsmdb.exception.ElementNotFoundException;
import com.lsmdb.exception.HardLimitBreachedException;
import io.vertx.core.buffer.Buffer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.util.SerializationUtils;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.Deque;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class LSMService {

        public static boolean hardLimitBreached = false;
        private final FileIOService fileIOService;
        private final SegmentService segmentService;
        private final MergeService mergeService;
        private final Deque<SegmentIndex> indices;
        private final MemTableWrapper memTable;

        public LSMService(MemTableWrapper memTableWrapper,
                          Deque<SegmentIndex> indices, FileIOService fileIOService,
                          SegmentService segmentService, MergeService mergeService
        ) {
                this.fileIOService = fileIOService;
                this.segmentService = segmentService;
                this.mergeService = mergeService;
                this.indices = indices;
                this.memTable = memTableWrapper;
        }

        @Scheduled(initialDelay = 10000, fixedDelay = 30000)
        public void merge() throws IOException {
                log.info("**************\nStarting scheduled merging!\n******************");
                final var segmentEnumeration = getSegmentIndexEnumeration();
                var segmentIndexCountToBeRemoved = segmentEnumeration.size();
                var mergeSegment = segmentService.getNewSegment();
                final var validSegmentEnumeration = segmentEnumeration.stream()
                        .filter(i -> new File(
                                segmentService.getPathForSegment(i.getRight().getSegment().getSegmentName())).exists())
                        .toList();
                if (validSegmentEnumeration.size() > 10) {
                        var mergedSegmentIndex =
                                mergeService.merge(validSegmentEnumeration, mergeSegment.getSegmentPath());

                        indices.addLast(new SegmentIndex(mergeSegment, mergedSegmentIndex));
                        IntStream.range(0, segmentIndexCountToBeRemoved)
                                .forEach(x -> indices.removeAll(getIndicesForMergedSegments(validSegmentEnumeration)));

                        fileIOService.persistIndex(mergeSegment.getBackupPath(), SerializationUtils.serialize(indices));
                        deleteMergedSegments(segmentEnumeration);
                }
        }

        private List<SegmentIndex> getIndicesForMergedSegments(
                List<ImmutablePair<Enumeration<String>, SegmentIndex>> segmentEnumeration) {
                return segmentEnumeration.stream().map(ImmutablePair::getRight).collect(Collectors.toList());
        }

        private void deleteMergedSegments(
                final List<ImmutablePair<Enumeration<String>, SegmentIndex>> segmentIndexEnumeration) {
                segmentIndexEnumeration.parallelStream().map(x -> x.getRight().getSegment())
                        .forEach(segment -> {
                                try {
                                        new File(segment.getBackupPath()).delete();
                                        new File(segment.getSegmentPath()).delete();
                                } catch (RuntimeException exception) {
                                        exception.printStackTrace();
                                }
                        });
        }

        public List<ImmutablePair<Enumeration<String>, SegmentIndex>> getSegmentIndexEnumeration() {
                return indices.stream()
                        .map(j -> ImmutablePair.of(Collections.enumeration(j.getIndex().keySet()), j)).toList();
        }

        public CompletableFuture<Void> insert(final String id, final Instant timestamp, final Buffer payload) {
                if (hardLimitBreached) {
                        throw new HardLimitBreachedException("All write requests will be ignored " +
                                "until memory becomes available!");
                }
                return memTable.persist(id, timestamp, payload);
        }

        public String getData(String id) throws ElementNotFoundException {
                var data = memTable.getData(id);
                return data.orElseThrow(() -> new ElementNotFoundException(String.format("id - %s not found!", id)));
        }
}
