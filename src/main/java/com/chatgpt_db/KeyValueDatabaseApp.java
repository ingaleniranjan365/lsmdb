package com.chatgpt_db;

import spark.Spark;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class KeyValueDatabaseApp {
        static final String DATA_DIR = "database_data";

        public static void main(String[] args) {
                createDataDirectory();

                Spark.port(8080);

                KeyValueDatabase database = new KeyValueDatabase();

                Spark.put("/element/:id/timestamp/:timestamp", (req, res) -> {
                        String id = req.params("id");
                        String timestampString = req.params("timestamp");
                        String requestBody = req.body();

                        database.put(id, timestampString, requestBody);
                        return "Data inserted successfully";
                });

                Spark.get("/latest/element/:id", (req, res) -> {
                        String id = req.params("id");
                        String latestValue = database.getLatest(id);

                        if (latestValue != null) {
                                res.type("application/json");
                                return latestValue;
                        } else {
                                res.status(404);
                                return "Key not found";
                        }
                });

                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                        database.saveData();
                }));

                database.loadData();

                Spark.awaitInitialization();
        }

        private static void createDataDirectory() {
                try {
                        Files.createDirectories(Path.of(DATA_DIR));
                } catch (IOException e) {
                        e.printStackTrace();
                }
        }
}

class KeyValueDatabase {
        private final Map<String, ConcurrentSkipListMap<Long, String>> data;
        private final Map<String, Lock> locks;
        private final Map<String, Long> lastTimestamps;
        private final int maxInMemoryValues;

        public KeyValueDatabase() {
                data = new ConcurrentHashMap<>();
                locks = new ConcurrentHashMap<>();
                lastTimestamps = new ConcurrentHashMap<>();
                maxInMemoryValues = 25000;  // 25% of 100k unique keys
        }

        public void put(String id, String timestampString, String value) {
                long timestamp = Instant.parse(timestampString).toEpochMilli();
                locks.computeIfAbsent(id, k -> new ReentrantLock()).lock();
                try {
                        lastTimestamps.put(id, Math.max(lastTimestamps.getOrDefault(id, 0L), timestamp));
                        data.computeIfAbsent(id, k -> new ConcurrentSkipListMap<>())
                                .put(timestamp, value);
                        if (data.get(id).size() > maxInMemoryValues) {
                                data.get(id).remove(data.get(id).firstKey());
                        }
                } finally {
                        locks.get(id).unlock();
                }
        }

        public String getLatest(String id) {
                if (lastTimestamps.containsKey(id)) {
                        long latestTimestamp = lastTimestamps.get(id);
                        return data.get(id).get(latestTimestamp);
                }
                return null;
        }

        public synchronized void saveData() {
                for (Map.Entry<String, ConcurrentSkipListMap<Long, String>> entry : data.entrySet()) {
                        String id = entry.getKey();
                        ConcurrentSkipListMap<Long, String> keyData = entry.getValue();

                        Path filePath = Path.of(KeyValueDatabaseApp.DATA_DIR, id + ".dat");
                        try (BufferedWriter writer = Files.newBufferedWriter(filePath, StandardOpenOption.CREATE)) {
                                for (Map.Entry<Long, String> dataEntry : keyData.entrySet()) {
                                        writer.write(dataEntry.getKey() + "|" + dataEntry.getValue() + "\n");
                                }
                        } catch (IOException e) {
                                e.printStackTrace();
                        }
                }
        }

        public synchronized void loadData() {
                try {
                        Files.list(Path.of(KeyValueDatabaseApp.DATA_DIR))
                                .filter(Files::isRegularFile)
                                .forEach(filePath -> {
                                        String id = filePath.getFileName().toString().replace(".dat", "");
                                        ConcurrentSkipListMap<Long, String> keyData = new ConcurrentSkipListMap<>();

                                        try (BufferedReader reader = Files.newBufferedReader(filePath)) {
                                                String line;
                                                while ((line = reader.readLine()) != null) {
                                                        String[] parts = line.split("\\|");
                                                        long timestamp = Long.parseLong(parts[0]);
                                                        String value = parts[1];
                                                        keyData.put(timestamp, value);
                                                        lastTimestamps.put(id, Math.max(lastTimestamps.getOrDefault(id, 0L), timestamp));
                                                }
                                        } catch (IOException e) {
                                                e.printStackTrace();
                                        }

                                        data.put(id, keyData);
                                });
                } catch (IOException e) {
                        e.printStackTrace();
                }
        }
}
