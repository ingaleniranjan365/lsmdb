package com.lsmdb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lsmdb.entity.SegmentIndex;
import com.lsmdb.service.FileIOService;
import io.vertx.core.buffer.Buffer;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Arrays;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class StateLoader {

        private final FileIOService fileIOService;
        private final String segmentsPath;
        private final String configPath;
        private final String walPath;
        private final ObjectMapper mapper = new ObjectMapper();

        public StateLoader(final FileIOService fileIOService, final String segmentsPath, final String configPath,
                           final String walPath) {
                this.fileIOService = fileIOService;
                this.segmentsPath = segmentsPath;
                this.configPath = configPath;
                this.walPath = walPath;
        }

        public SegmentConfig getSegmentConfig() {
                var segmentConfig = fileIOService.getSegmentConfig(configPath);
                if (segmentConfig.isPresent()) {
                        var config = segmentConfig.get();
                        config.setSegmentsPath(segmentsPath);
                        return config;
                }
                var scratchSegmentConf = new SegmentConfig(segmentsPath, -1);
                persistScratchSegmentConf(scratchSegmentConf);
                return scratchSegmentConf;
        }

        private void persistScratchSegmentConf(SegmentConfig scratchSegmentConf) {
                var configFile = new File(configPath);
                if (!configFile.getParentFile().exists() && !configFile.getParentFile().mkdirs()) {
                        throw new RuntimeException("Failed to create data directory!");
                }
                fileIOService.persistConfig(configPath, scratchSegmentConf);
        }

        public Deque<SegmentIndex> getIndices() {
                var segmentConfig = fileIOService.getSegmentConfig(configPath);
                var indices = new ConcurrentLinkedDeque<SegmentIndex>();
                if (segmentConfig.isPresent()) {
                        var counter = segmentConfig.get().getCount();
                        while (counter >= 0) {
                                var index = fileIOService.getIndex(segmentsPath + "/indices/index-" + counter);
                                index.ifPresent(indices::addLast);
                                counter--;
                        }
                }
                return indices;
        }

        public ConcurrentSkipListMap<String, ImmutablePair<Instant, Buffer>> getMemTableFromWAL() {
                try {
                        var wal = new RandomAccessFile(walPath, "r");
                        var walLen = wal.length();
                        var recordSize = fileIOService.getRecordSize();
                        var inMemoryRecordsCnt = fileIOService.getInMemoryRecordCntHardLimit();
                        var walSizeToRead = inMemoryRecordsCnt * recordSize;
                        var startOffset = Math.max(0, walLen - walSizeToRead);
                        var walBytes = new byte[walSizeToRead];
                        wal.seek(startOffset);
                        wal.readFully(walBytes);
                        var records = IntStream.range(0, inMemoryRecordsCnt)
                                .parallel()
                                .mapToObj(i -> {
                                        int startIndex = i * recordSize;
                                        int endIndex = Math.min(startIndex + recordSize, walBytes.length);
                                        return Arrays.copyOfRange(walBytes, startIndex, endIndex);
                                }).map(
                                        b -> {
                                                try {
                                                        var payloadLen = ByteBuffer.wrap(b, 0, Integer.BYTES).getInt();
                                                        var payloadBytes = Arrays.copyOfRange(b, Integer.BYTES,
                                                                payloadLen + Integer.BYTES);
                                                        var timestamp =
                                                                Instant.parse(mapper.readTree(payloadBytes).get(
                                                                        "timestamp").toString().replace("\"", ""));
                                                        var id = mapper.readTree(payloadBytes).get("id").toString()
                                                                .replace("\"", "");
                                                        return ImmutablePair.of(id, ImmutablePair.of(timestamp,
                                                                Buffer.buffer(payloadBytes)));
                                                } catch (IOException e) {
                                                        e.printStackTrace();
                                                }
                                                return ImmutablePair.of("",
                                                        ImmutablePair.of(Instant.MIN, Buffer.buffer("")));
                                        }
                                ).collect(Collectors.toMap(
                                        ImmutablePair::getLeft,
                                        ImmutablePair::getRight,
                                        (existingValue, newValue) -> {
                                                Instant existingTimestamp = existingValue.getLeft();
                                                Instant newTimestamp = newValue.getLeft();
                                                return existingTimestamp.isAfter(newTimestamp) ? existingValue : newValue;
                                        }
                                ));
                        return new ConcurrentSkipListMap<>(records);
                } catch (RuntimeException | IOException e) {
                        e.printStackTrace();
                }
                return new ConcurrentSkipListMap<>();
        }

}
