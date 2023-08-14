package com.chatgpt_db;

import com.google.gson.Gson;
import spark.Spark;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class KeyValueDatabaseApp {
        static final String DATA_DIR = "database_data";

        public static void main(String[] args) {
                createDataDirectory();

                Spark.port(8080); // Set the port to 8080

                KeyValueDatabase database = new KeyValueDatabase();
                Gson gson = new Gson();

                // PUT endpoint
                Spark.put("/element/:id/timestamp/:timestamp", (req, res) -> {
                        String id = req.params("id");
                        String timestampString = req.params("timestamp");
                        Instant instant = Instant.parse(timestampString);  // Parse as Instant
                        LocalDateTime timestamp = instant.atZone(ZoneOffset.UTC).toLocalDateTime(); // Convert to LocalDateTime
                        String requestBody = req.body();

                        database.put(id, timestamp, requestBody);
                        return "Data inserted successfully";
                });

                // GET endpoint
                Spark.get("/latest/element/:id", (req, res) -> {
                        String id = req.params("id");
                        String latestValue = database.getLatest(id);

                        if (latestValue != null) {
                                res.type("application/json"); // Set the response type to JSON
                                return latestValue; // Return the serialized JSON object directly
                        } else {
                                res.status(404); // Set the status to indicate resource not found
                                return "Key not found";
                        }
                });

                // Save data on shutdown
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                        database.saveData();
                }));

                // Load data on startup
                database.loadData();

                // Start Spark server
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

class   KeyValueDatabase {
        private final Map<String, ConcurrentSkipListMap<LocalDateTime, String>> data;
        private final Map<String, Lock> locks;

        public KeyValueDatabase() {
                data = new ConcurrentHashMap<>();
                locks = new ConcurrentHashMap<>();
        }

        public void put(String id, LocalDateTime timestamp, String value) {
                locks.computeIfAbsent(id, k -> new ReentrantLock()).lock();
                try {
                        data.computeIfAbsent(id, k -> new ConcurrentSkipListMap<>()).put(timestamp, value);
                } finally {
                        locks.get(id).unlock();
                }
        }

        public String getLatest(String id) {
                ConcurrentSkipListMap<LocalDateTime, String> keyData = data.get(id);
                if (keyData != null && !keyData.isEmpty()) {
                        return keyData.lastEntry().getValue();
                }
                return null;
        }

        public synchronized void saveData() {
                for (Map.Entry<String, ConcurrentSkipListMap<LocalDateTime, String>> entry : data.entrySet()) {
                        String id = entry.getKey();
                        ConcurrentSkipListMap<LocalDateTime, String> keyData = entry.getValue();

                        Path filePath = Path.of(KeyValueDatabaseApp.DATA_DIR, id + ".dat");
                        try (BufferedWriter writer = Files.newBufferedWriter(filePath, StandardOpenOption.CREATE)) {
                                for (Map.Entry<LocalDateTime, String> dataEntry : keyData.entrySet()) {
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
                                        ConcurrentSkipListMap<LocalDateTime, String> keyData = new ConcurrentSkipListMap<>();

                                        try (BufferedReader reader = Files.newBufferedReader(filePath)) {
                                                String line;
                                                while ((line = reader.readLine()) != null) {
                                                        String[] parts = line.split("\\|");
                                                        LocalDateTime timestamp = LocalDateTime.parse(parts[0]);
                                                        String value = parts[1];
                                                        keyData.put(timestamp, value);
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
