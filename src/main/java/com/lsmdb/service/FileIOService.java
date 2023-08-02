package com.lsmdb.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lsmdb.SegmentConfig;
import com.lsmdb.entity.Segment;
import com.lsmdb.entity.SegmentIndex;
import com.lsmdb.entity.SegmentMetadata;
import io.vertx.core.buffer.Buffer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.springframework.util.SerializationUtils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.RandomAccessFile;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.CompletableFuture.supplyAsync;

@Slf4j
public class FileIOService {

        public static final ObjectMapper mapper = new ObjectMapper();

        public SegmentIndex persist(
                final Segment segment,
                final Deque<String> ids,
                final Map<String, Deque<Buffer>> memTable,
                final ImmutablePair<Integer, Integer> range
        ) {
                final Map<String, SegmentMetadata> index = new HashMap<>();
                var segmentBuffer = Buffer.buffer();
                ids.stream().toList().subList(range.left, range.right).stream().sorted()
                        .forEach(p -> {
                                try {
                                        var list = memTable.getOrDefault(p, null);
                                        if (list != null) {
                                                var payload = list.removeFirst();
                                                index.put(p,
                                                        new SegmentMetadata(segmentBuffer.length(), payload.length()));
                                                segmentBuffer.appendBuffer(payload);
                                        }
                                } catch (RuntimeException ex) {
                                        ex.printStackTrace();
                                }
                        });
                File segmentFile = new File(segment.getSegmentPath());
                try {
                        FileUtils.writeByteArrayToFile(segmentFile, segmentBuffer.getBytes(), true);
                } catch (IOException e) {
                        e.printStackTrace();
                }
                final var segmentIndex = new SegmentIndex(segment, index);
                persistIndex(segment.getBackupPath(), SerializationUtils.serialize(segmentIndex));
                return segmentIndex;
        }

        public void persistConfig(final String configPath, final SegmentConfig config) {
                try {
                        var json = mapper.writeValueAsString(config);
                        FileOutputStream outputStream = new FileOutputStream(configPath);
                        outputStream.write(json.getBytes());
                        outputStream.close();
                } catch (IOException exception) {
                        exception.printStackTrace();
                }
        }

        public byte[] readBytes(final String path, final SegmentMetadata metadata) throws IOException {
                RandomAccessFile raf = null;
                raf = new RandomAccessFile(path, "r");
                raf.seek(metadata.getOffset());
                byte[] in = new byte[(int) metadata.getSize()];
                raf.read(in, 0, (int) metadata.getSize());
                raf.close();
                return in;
        }

        public Optional<SegmentConfig> getSegmentConfig(final String path) {
                try {
                        return Optional.of(
                                mapper.readValue(new File(path), SegmentConfig.class));
                } catch (IOException e) {
                        e.printStackTrace();
                        return Optional.empty();
                }
        }

        public Optional<SegmentIndex> getIndex(String path) {
                try {
                        File file = new File(path);
                        var in = FileUtils.readFileToByteArray(file);
                        ByteArrayInputStream bis = new ByteArrayInputStream(in);
                        ObjectInputStream ois = new ObjectInputStream(bis);
                        return Optional.of((SegmentIndex) ois.readObject());
                } catch (IOException | ClassNotFoundException e) {
                        e.printStackTrace();
                        return Optional.empty();
                }
        }

        public Optional<String> getPayload(final String path, final SegmentMetadata metadata) {
                try {
                        var in = readBytes(path, metadata);
                        return Optional.of(new String(in));
                } catch (IOException e) {
                        e.printStackTrace();
                        return Optional.empty();
                }
        }

        public void persistIndex(final String newBackupPath, final byte[] indicesBytes) {
                try {
                        var newIndexFile = new File(newBackupPath);
                        FileUtils.writeByteArrayToFile(newIndexFile, indicesBytes);
                } catch (IOException | RuntimeException ex) {
                        ex.printStackTrace();
                }
        }

        public CompletableFuture<Void> writeAheadLog(Buffer payload) {
                // TODO : implement write ahead log, this is a dummy impl
                return supplyAsync(() -> null);
        }

}
