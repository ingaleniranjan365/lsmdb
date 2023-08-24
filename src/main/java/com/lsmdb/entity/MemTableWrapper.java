package com.lsmdb.entity;

import com.lsmdb.service.FileIOService;
import com.lsmdb.service.SegmentService;
import io.vertx.core.buffer.Buffer;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.time.Instant;
import java.util.Deque;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.CompletableFuture.supplyAsync;

@Getter
@Setter
@Slf4j
public class MemTableWrapper {

        public static boolean hardLimitBreached = false;
        private final Lock memTableLock = new ReentrantLock();
        private final int memTableSoftLimit;
        private final int memTableHardLimit;
        private Deque<SegmentIndex> segmentIndices;
        private FileIOService fileIOService;
        private SegmentService segmentService;
        private Map<String, ImmutablePair<Instant, Buffer>> memTable;
        private Map<String, ImmutablePair<Instant, Buffer>> readOnlyMemTable;

        public MemTableWrapper(
                Map<String, ImmutablePair<Instant, Buffer>> memTable,
                Deque<SegmentIndex> indices,
                FileIOService fileIOService,
                SegmentService segmentService, int inMemoryRecordCntSoftLimit, int inMemoryRecordCntHardLimit) {
                this.segmentIndices = indices;
                this.fileIOService = fileIOService;
                this.memTable = memTable;
                this.readOnlyMemTable = memTable;
                this.segmentService = segmentService;
                this.memTableSoftLimit = inMemoryRecordCntSoftLimit;
                this.memTableHardLimit = inMemoryRecordCntHardLimit;
        }

        public CompletableFuture<Void> persist(final String id, final Instant timestamp, final Buffer payload) {
                return supplyAsync(() -> fileIOService.writeAheadLog(payload))
                        .thenAccept(walSuccess -> {
                                if (!walSuccess) {
                                        log.error("Write Ahead Log failed for id: " + id);
                                }
                        })
                        .thenRun(() -> put(id, timestamp, payload))
                        .thenRunAsync(this::update);
        }

        public CompletableFuture<Void> persistCopy(final String id, final Instant timestamp, final Buffer payload) {
                return supplyAsync(() -> fileIOService.writeAheadLog(payload))
                        .thenComposeAsync(walSuccess -> {
                                if (!walSuccess) {
                                        log.error("Write Ahead Log failed for id: " + id);
                                }
                                return CompletableFuture.completedFuture(null);
                        })
                        .thenComposeAsync(unused -> CompletableFuture.runAsync(() -> put(id, timestamp, payload)))
                        .thenComposeAsync(unused -> CompletableFuture.runAsync(this::update));
        }

        private void update() {
                if (memTableLock.tryLock()) {
                        try {
                                final var size = memTable.size();
                                final var isMemTableFull = size >= memTableSoftLimit;
                                if (isMemTableFull) {
                                        memTable = new ConcurrentSkipListMap<>();
                                        hardLimitBreached = size >= memTableHardLimit;

                                        var segment = segmentService.getNewSegment();
                                        var index = fileIOService.persist(segment, readOnlyMemTable);
                                        segmentIndices.addFirst(index);
                                        readOnlyMemTable = memTable;
                                }

                        } catch (Exception ex) {
                                throw new RuntimeException(ex);
                        } finally {
                                memTableLock.unlock();
                        }
                }
        }

        private void put(final String id, final Instant timestamp, final Buffer payload) {
                var prevTimestamp = get(id).orElse(ImmutablePair.of(timestamp.minusSeconds(1), null)).left;
                if (timestamp.isAfter(prevTimestamp) || timestamp.equals(prevTimestamp)) {
                        memTable.put(id, ImmutablePair.of(timestamp, payload));
                }
        }

        private Optional<ImmutablePair<Instant, Buffer>> get(final String id) {
                return Optional.ofNullable(memTable.get(id))
                        .or(() -> Stream.of(readOnlyMemTable)
                                .filter(x -> x.containsKey(id))
                                .findFirst()
                                .map(r -> r.get(id))
                        )
                        .or(() -> getDataFromSegments(id));
        }

        private Optional<ImmutablePair<Instant, Buffer>> getDataFromSegments(final String id) {
                var segmentIndex =
                        segmentIndices.stream().filter(x -> x.getIndex().containsKey(id)).findFirst();
                if (segmentIndex.isPresent()) {
                        var metadata = segmentIndex.get().getIndex().get(id);
                        var instant = metadata.getInstant();
                        var segment = segmentIndex.get().getSegment();
                        var segmentPath = segment.getSegmentPath();
                        var payload = fileIOService.getPayload(segmentPath, metadata);
                        if (payload.isPresent()) {
                                return Optional.of(ImmutablePair.of(instant, payload.get()));
                        }
                }
                return Optional.empty();
        }

        public Optional<String> getData(String id) {
                var data = get(id);
                return data.map(instantBufferImmutablePair -> instantBufferImmutablePair.right.toString());
        }
}
