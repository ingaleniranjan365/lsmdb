package com.lsmdb.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lsmdb.SegmentConfig;
import com.lsmdb.entity.Metadata;
import com.lsmdb.entity.Segment;
import com.lsmdb.entity.SegmentIndex;
import com.lsmdb.exception.PayloadTooLargeException;
import io.vertx.core.buffer.Buffer;
import lombok.Data;
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
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Data
public class FileIOService {

        private final ObjectMapper mapper = new ObjectMapper();
        private final File wal_file;
        private final Integer buffer_size;
        private final int metadataSize;
        private final int inMemoryRecordCntHardLimit;

        public FileIOService(final String walPath, final int buffer_size,
                             int inMemoryRecordCntHardLimit) {
                wal_file = new File(walPath);
                this.buffer_size = buffer_size;
                this.metadataSize = Integer.BYTES;
                this.inMemoryRecordCntHardLimit = inMemoryRecordCntHardLimit;
        }

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
                persistIndex(segment.getIndexPath(), SerializationUtils.serialize(segmentIndex));
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

        public void persistIndex(final String newIndexPath, final byte[] indicesBytes) {
                try {
                        var newIndexFile = new File(newIndexPath);
                        FileUtils.writeByteArrayToFile(newIndexFile, indicesBytes);
                } catch (IOException | RuntimeException ex) {
                        ex.printStackTrace();
                }
        }

        public boolean writeAheadLog(Buffer payload) {
                var len = payload.length();
                if (len > 1000) {
                        throw new PayloadTooLargeException("Payload larger than 1000 bytes");
                }
                var bytes = payload.getBytes();
                int paddingSize = Math.max(0, buffer_size - len);
                var byteBuffer = ByteBuffer.allocate(metadataSize + buffer_size);
                byteBuffer.putInt(len);
                byteBuffer.put(bytes);
                if (paddingSize > 0) {
                        byteBuffer.put(new byte[paddingSize]);
                }
                try {
                        FileUtils.writeByteArrayToFile(wal_file, byteBuffer.array(), true);
                        return true;
                } catch (IOException e) {
                        e.printStackTrace();
                        return false;
                }
        }

        public int getRecordSize() {
                return metadataSize + buffer_size;
        }
}
