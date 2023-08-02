package com.lsmdb.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lsmdb.SegmentConfig;
import com.lsmdb.entity.Metadata;
import com.lsmdb.entity.Segment;
import com.lsmdb.entity.SegmentIndex;
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
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Slf4j
public class FileIOService {

        public static final ObjectMapper mapper = new ObjectMapper();

        public SegmentIndex persist(
                final Segment segment,
                final Map<String, ImmutablePair<Instant, Buffer>> memTable
        ) {
                final Map<String, Metadata> index = new HashMap<>();
                var segmentBuffer = Buffer.buffer();

                for (Map.Entry<String, ImmutablePair<Instant, Buffer>> entry : memTable.entrySet()) {
                        var id = entry.getKey();
                        var instant = entry.getValue().left;
                        var value = entry.getValue().right;
                        var metadata = new Metadata(segmentBuffer.length(), value.length(), instant);
                        segmentBuffer.appendBuffer(value);
                        index.put(id, metadata);
                }

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

        public byte[] readBytes(final String path, final Metadata metadata) throws IOException {
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

        public Optional<Buffer> getPayload(final String path, final Metadata metadata) {
                try {
                        var in = readBytes(path, metadata);
                        return Optional.of(Buffer.buffer(in));
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

        public boolean writeAheadLog(Buffer payload) {
                // TODO : implement write ahead log, this is a dummy impl
                return true;
        }

}
