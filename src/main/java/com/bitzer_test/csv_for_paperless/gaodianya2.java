package com.bitzer_test.csv_for_paperless;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class gaodianya2 {
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Create a Kafka source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("your_topic_name")
                .setGroupId("your_group_id")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();

        // Read from Kafka
        DataStream<String> stream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // Parse JSON and combine Step 1 and Step 2 into one record
        DataStream<Map<String, String>> combinedStream = stream
                .filter(msg -> !msg.contains("Test_GUID")) // Skip the header
                .map(new RichMapFunction<String, Map<String, String>>() {
                    private transient ObjectMapper mapper;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        mapper = new ObjectMapper();
                    }

                    @Override
                    public Map<String, String> map(String value) throws Exception {
                        JsonNode json = mapper.readTree(value);

                        // Extract fields for Step 1
                        Map<String, String> step1 = extractFields(json.get(1));
                        // Extract fields for Step 2
                        Map<String, String> step2 = extractFields(json.get(2));

                        // Combine both steps into one record
                        Map<String, String> combinedRecord = new HashMap<>(step1);
                        combinedRecord.putAll(addSuffix(step2, "2"));

                        return combinedRecord;
                    }

                    private Map<String, String> extractFields(JsonNode json) {
                        if (json == null || !json.isObject()) {
                            return null;
                        }
                        return Map.of(
                                "Test_GUID", json.path("Test_GUID").asText(),
                                "Step", json.path("Step").asText(),
                                "Test_step_definition", json.path("Test_step_definition").asText(),
                                "Operator", json.path("Operator").asText(),
                                "Date", json.path("Date").asText(),
                                "Time", json.path("Time").asText(),
                                "Timestamp", json.path("Timestamp").asText()
                        );
                    }

                    private Map<String, String> addSuffix(Map<String, String> map, String suffix) {
                        Map<String, String> result = new HashMap<>();
                        map.forEach((key, value) -> result.put(key + suffix, value));
                        return result;
                    }
                })
                .filter(Objects::nonNull); // Remove any null results

        // Define the JDBC sink
        SinkFunction<Map<String, String>> jdbcSink = JdbcSink.sink(
                "INSERT INTO Test_Results (Test_GUID, Step, Test_step_definition, Operator, Date, Time, Timestamp, " +
                        "Test_GUID2, Step2, Test_step_definition2, Operator2, Date2, Time2, Timestamp2) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (statement, record) -> {
                    statement.setString(1, record.get("Test_GUID"));
                    statement.setString(2, record.get("Step"));
                    statement.setString(3, record.get("Test_step_definition"));
                    statement.setString(4, record.get("Operator"));
                    statement.setDate(5, java.sql.Date.valueOf(LocalDate.parse(record.get("Date"), java.time.format.DateTimeFormatter.ofPattern("dd/MM/yyyy"))));
                    statement.setTime(6, java.sql.Time.valueOf(LocalTime.parse(record.get("Time"), java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"))));
                    statement.setTimestamp(7, java.sql.Timestamp.valueOf(LocalDateTime.parse(record.get("Timestamp"), java.time.format.DateTimeFormatter.ofPattern("MM/dd/yyyy h:mm:ss a"))));

                    statement.setString(8, record.get("Test_GUID2"));
                    statement.setString(9, record.get("Step2"));
                    statement.setString(10, record.get("Test_step_definition2"));
                    statement.setString(11, record.get("Operator2"));
                    statement.setDate(12, java.sql.Date.valueOf(LocalDate.parse(record.get("Date2"), java.time.format.DateTimeFormatter.ofPattern("dd/MM/yyyy"))));
                    statement.setTime(13, java.sql.Time.valueOf(LocalTime.parse(record.get("Time2"), java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"))));
                    statement.setTimestamp(14, java.sql.Timestamp.valueOf(LocalDateTime.parse(record.get("Timestamp2"), java.time.format.DateTimeFormatter.ofPattern("MM/dd/yyyy h:mm:ss a"))));
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:sqlserver://your_sql_server:1433;databaseName=YourDatabase")
                        .withDriverName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
                        .withUsername("your_username")
                        .withPassword("your_password")
                        .build()
        );

        // Write to SQL Server
        combinedStream.addSink(jdbcSink);

        // Execute the program
        env.execute("Kafka to SQL Server");
    }
}
