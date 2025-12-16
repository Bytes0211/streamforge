package com.streamforge;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main Flink streaming job that processes data from Kafka and writes to MongoDB
 */
public class StreamProcessor {
    
    private static final Logger LOG = LoggerFactory.getLogger(StreamProcessor.class);
    
    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Configure Kafka source
        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers("kafka:29092")
            .setTopics("streamforge-input")
            .setGroupId("streamforge-consumer-group")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();
        
        // Create data stream from Kafka
        DataStream<String> kafkaStream = env.fromSource(
            source,
            WatermarkStrategy.noWatermarks(),
            "Kafka Source"
        );
        
        // Process the stream
        DataStream<String> processed = kafkaStream
            .map(value -> {
                LOG.info("Processing message: {}", value);
                // TODO: Add your processing logic here
                return value.toUpperCase();
            })
            .name("Transform Data");
        
        // Sink to MongoDB
        processed.addSink(new MongoDBSink())
            .name("MongoDB Sink");
        
        // Print to console for debugging
        processed.print();
        
        // Execute the job
        env.execute("StreamForge Processor");
    }
}
