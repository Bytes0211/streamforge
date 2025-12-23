package com.streamforge;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamforge.model.AggregatedMetrics;
import com.streamforge.model.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Enhanced Flink streaming job with JSON processing, stateful operations,
 * windowing, aggregations, and error handling
 */
public class StreamProcessor {
    
    private static final Logger LOG = LoggerFactory.getLogger(StreamProcessor.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    // Dead letter queue tag for error handling
    private static final OutputTag<String> DLQ_TAG = new OutputTag<String>("dead-letter-queue"){};
    
    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Enable checkpointing for fault tolerance
        env.enableCheckpointing(30000); // Checkpoint every 30 seconds
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setMinPauseBetweenCheckpoints(10000);
        checkpointConfig.setCheckpointTimeout(60000);
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.setExternalizedCheckpointCleanup(
            CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        
        // Configure Kafka source
        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers("kafka:29092")
            .setTopics("streamforge-input")
            .setGroupId("streamforge-consumer-group")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();
        
        // Create data stream from Kafka with watermarks
        DataStream<String> kafkaStream = env.fromSource(
            source,
            WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, timestamp) -> System.currentTimeMillis()),
            "Kafka Source"
        );
        
        // Parse JSON and validate events (with side output for errors)
        SingleOutputStreamOperator<Event> parsedStream = kafkaStream
            .process(new ProcessFunction<String, Event>() {
                @Override
                public void processElement(String value, Context ctx, Collector<Event> out) {
                    try {
                        Event event = objectMapper.readValue(value, Event.class);
                        if (event.isValid()) {
                            out.collect(event);
                            LOG.debug("Successfully parsed event: {}", event.getId());
                        } else {
                            LOG.warn("Invalid event: {}", value);
                            ctx.output(DLQ_TAG, value);
                        }
                    } catch (Exception e) {
                        LOG.error("Failed to parse JSON: {}", value, e);
                        ctx.output(DLQ_TAG, value);
                    }
                }
            })
            .name("Parse and Validate JSON");
        
        // Extract dead letter queue for error handling
        DataStream<String> deadLetterQueue = parsedStream.getSideOutput(DLQ_TAG);
        deadLetterQueue.addSink(new DeadLetterQueueSink())
            .name("Dead Letter Queue Sink");
        
        // Assign watermarks based on event timestamp
        DataStream<Event> eventsWithWatermarks = parsedStream
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
            );
        
        // Stateful processing: Track event count per user
        DataStream<Event> enrichedStream = eventsWithWatermarks
            .keyBy(Event::getUserId)
            .process(new EventEnrichmentFunction())
            .name("Stateful Enrichment");
        
        // Window-based aggregations: 1-minute tumbling windows
        DataStream<AggregatedMetrics> aggregatedStream = enrichedStream
            .keyBy(event -> event.getUserId() + ":" + event.getType())
            .window(TumblingEventTimeWindows.of(Time.minutes(1)))
            .aggregate(new EventAggregationFunction())
            .name("Windowed Aggregations");
        
        // Sink enriched events to MongoDB
        enrichedStream.addSink(new MongoDBSink())
            .name("MongoDB Event Sink");
        
        // Sink aggregated metrics to MongoDB
        aggregatedStream.addSink(new MongoDBMetricsSink())
            .name("MongoDB Metrics Sink");
        
        // Print streams for debugging
        enrichedStream.print("Events");
        aggregatedStream.print("Metrics");
        
        // Execute the job
        env.execute("StreamForge Processor - Enhanced");
    }
    
    /**
     * Stateful function to enrich events with user-specific counters
     */
    private static class EventEnrichmentFunction extends KeyedProcessFunction<String, Event, Event> {
        private transient ValueState<Long> eventCountState;
        
        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>(
                "event-count",
                TypeInformation.of(Long.class)
            );
            eventCountState = getRuntimeContext().getState(descriptor);
        }
        
        @Override
        public void processElement(Event event, Context ctx, Collector<Event> out) throws Exception {
            Long currentCount = eventCountState.value();
            if (currentCount == null) {
                currentCount = 0L;
            }
            currentCount++;
            eventCountState.update(currentCount);
            
            LOG.debug("User {} has {} total events", event.getUserId(), currentCount);
            out.collect(event);
        }
    }
    
    /**
     * Aggregate function to compute metrics over time windows
     */
    private static class EventAggregationFunction 
            implements AggregateFunction<Event, EventAccumulator, AggregatedMetrics> {
        
        @Override
        public EventAccumulator createAccumulator() {
            return new EventAccumulator();
        }
        
        @Override
        public EventAccumulator add(Event event, EventAccumulator acc) {
            acc.userId = event.getUserId();
            acc.eventType = event.getType();
            acc.count++;
            acc.sum += event.getValue();
            acc.min = Math.min(acc.min, event.getValue());
            acc.max = Math.max(acc.max, event.getValue());
            
            if (acc.windowStart == 0) {
                acc.windowStart = event.getTimestamp();
            }
            acc.windowEnd = event.getTimestamp();
            
            return acc;
        }
        
        @Override
        public AggregatedMetrics getResult(EventAccumulator acc) {
            double avg = acc.count > 0 ? acc.sum / acc.count : 0.0;
            return new AggregatedMetrics(
                acc.userId,
                acc.eventType,
                acc.count,
                acc.sum,
                avg,
                acc.min,
                acc.max,
                acc.windowStart,
                acc.windowEnd
            );
        }
        
        @Override
        public EventAccumulator merge(EventAccumulator a, EventAccumulator b) {
            a.count += b.count;
            a.sum += b.sum;
            a.min = Math.min(a.min, b.min);
            a.max = Math.max(a.max, b.max);
            a.windowStart = Math.min(a.windowStart, b.windowStart);
            a.windowEnd = Math.max(a.windowEnd, b.windowEnd);
            return a;
        }
    }
    
    /**
     * Accumulator for aggregation function
     */
    private static class EventAccumulator {
        String userId;
        String eventType;
        long count = 0;
        double sum = 0.0;
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;
        long windowStart = 0;
        long windowEnd = 0;
    }
}
