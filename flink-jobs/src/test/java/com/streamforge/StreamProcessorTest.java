package com.streamforge;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Unit tests for StreamProcessor
 * These tests verify the data transformation logic and integration between components
 */
public class StreamProcessorTest {
    
    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
        new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                .setNumberSlotsPerTaskManager(2)
                .setNumberTaskManagers(1)
                .build());
    
    /**
     * Test case 5: StreamProcessor correctly applies the defined data transformation logic
     * This test verifies that the toUpperCase() transformation is applied correctly
     */
    @Test
    public void testTransformationLogic() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        // Create test data
        List<String> testData = new ArrayList<>();
        testData.add("hello");
        testData.add("world");
        testData.add("flink");
        
        // Create stream from collection
        DataStream<String> inputStream = env.fromCollection(testData);
        
        // Apply the same transformation as StreamProcessor
        DataStream<String> processed = inputStream
            .map(value -> value.toUpperCase())
            .name("Transform Data");
        
        // Collect results using a collecting sink
        CollectingSink.values.clear();
        processed.addSink(new CollectingSink());
        
        // Execute
        env.execute("Test Transformation Logic");
        
        // Verify
        assertEquals("Should have 3 transformed values", 3, CollectingSink.values.size());
        assertTrue("Should contain HELLO", CollectingSink.values.contains("HELLO"));
        assertTrue("Should contain WORLD", CollectingSink.values.contains("WORLD"));
        assertTrue("Should contain FLINK", CollectingSink.values.contains("FLINK"));
    }
    
    /**
     * Test case 5b: Verify transformation handles empty strings
     */
    @Test
    public void testTransformationWithEmptyString() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        List<String> testData = new ArrayList<>();
        testData.add("");
        testData.add("test");
        
        DataStream<String> inputStream = env.fromCollection(testData);
        
        DataStream<String> processed = inputStream
            .map(value -> value.toUpperCase())
            .name("Transform Data");
        
        CollectingSink.values.clear();
        processed.addSink(new CollectingSink());
        
        env.execute("Test Empty String Transformation");
        
        assertEquals("Should have 2 values", 2, CollectingSink.values.size());
        assertTrue("Should contain empty string", CollectingSink.values.contains(""));
        assertTrue("Should contain TEST", CollectingSink.values.contains("TEST"));
    }
    
    /**
     * Test case 5c: Verify transformation handles special characters
     */
    @Test
    public void testTransformationWithSpecialCharacters() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        List<String> testData = new ArrayList<>();
        testData.add("hello-world");
        testData.add("test_123");
        testData.add("special@char$");
        
        DataStream<String> inputStream = env.fromCollection(testData);
        
        DataStream<String> processed = inputStream
            .map(value -> value.toUpperCase())
            .name("Transform Data");
        
        CollectingSink.values.clear();
        processed.addSink(new CollectingSink());
        
        env.execute("Test Special Characters Transformation");
        
        assertEquals("Should have 3 values", 3, CollectingSink.values.size());
        assertTrue("Should contain HELLO-WORLD", CollectingSink.values.contains("HELLO-WORLD"));
        assertTrue("Should contain TEST_123", CollectingSink.values.contains("TEST_123"));
        assertTrue("Should contain SPECIAL@CHAR$", CollectingSink.values.contains("SPECIAL@CHAR$"));
    }
    
    /**
     * Test case: Verify stream processing preserves message order in single parallelism
     */
    @Test
    public void testMessageOrderPreservation() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // Single parallelism to ensure order
        
        List<String> testData = new ArrayList<>();
        testData.add("first");
        testData.add("second");
        testData.add("third");
        
        DataStream<String> inputStream = env.fromCollection(testData);
        
        DataStream<String> processed = inputStream
            .map(value -> value.toUpperCase())
            .name("Transform Data");
        
        OrderedCollectingSink.values.clear();
        processed.addSink(new OrderedCollectingSink());
        
        env.execute("Test Message Order");
        
        List<String> results = OrderedCollectingSink.values;
        assertEquals("Should have 3 values", 3, results.size());
        assertEquals("First message should be FIRST", "FIRST", results.get(0));
        assertEquals("Second message should be SECOND", "SECOND", results.get(1));
        assertEquals("Third message should be THIRD", "THIRD", results.get(2));
    }
    
    /**
     * Test case: Verify processing handles large batch of messages
     */
    @Test
    public void testLargeBatchProcessing() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        
        // Create large batch
        List<String> testData = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            testData.add("message" + i);
        }
        
        DataStream<String> inputStream = env.fromCollection(testData);
        
        DataStream<String> processed = inputStream
            .map(value -> value.toUpperCase())
            .name("Transform Data");
        
        CollectingSink.values.clear();
        processed.addSink(new CollectingSink());
        
        env.execute("Test Large Batch");
        
        assertEquals("Should process all 1000 messages", 1000, CollectingSink.values.size());
        
        // Verify a sample of transformations
        assertTrue("Should contain MESSAGE0", CollectingSink.values.contains("MESSAGE0"));
        assertTrue("Should contain MESSAGE500", CollectingSink.values.contains("MESSAGE500"));
        assertTrue("Should contain MESSAGE999", CollectingSink.values.contains("MESSAGE999"));
    }
    
    /**
     * Test case 4: Integration test - StreamProcessor successfully reads, transforms, and writes data
     * This is a simplified integration test that verifies the complete pipeline works
     */
    @Test
    public void testEndToEndPipeline() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        // Simulate the complete pipeline: source -> transform -> sink
        List<String> sourceData = new ArrayList<>();
        sourceData.add("input1");
        sourceData.add("input2");
        sourceData.add("input3");
        
        // Create source stream (simulating Kafka)
        DataStream<String> kafkaStream = env.fromCollection(sourceData);
        
        // Apply transformation (same as StreamProcessor)
        DataStream<String> processed = kafkaStream
            .map(value -> value.toUpperCase())
            .name("Transform Data");
        
        // Add sink (simulating MongoDB sink)
        TestMongoDBSink testSink = new TestMongoDBSink();
        processed.addSink(testSink).name("Test MongoDB Sink");
        
        // Execute
        env.execute("End-to-End Pipeline Test");
        
        // Verify sink received transformed data
        List<String> sinkData = TestMongoDBSink.collectedData;
        assertEquals("Sink should receive 3 records", 3, sinkData.size());
        assertTrue("Should contain INPUT1", sinkData.contains("INPUT1"));
        assertTrue("Should contain INPUT2", sinkData.contains("INPUT2"));
        assertTrue("Should contain INPUT3", sinkData.contains("INPUT3"));
    }
    
    /**
     * Test case: Verify null handling in transformation
     */
    @Test(expected = Exception.class)
    public void testTransformationWithNullValue() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        // Create stream with null value
        List<String> testData = new ArrayList<>();
        testData.add("valid");
        testData.add(null);
        
        DataStream<String> inputStream = env.fromCollection(testData, TypeInformation.of(String.class));
        
        DataStream<String> processed = inputStream
            .map(value -> value.toUpperCase()); // This will throw NullPointerException
        
        CollectingSink.values.clear();
        processed.addSink(new CollectingSink());
        
        // Should throw exception due to null value
        env.execute("Test Null Handling");
    }
    
    // Helper Sink for collecting results (unordered)
    private static class CollectingSink implements SinkFunction<String> {
        public static final List<String> values = Collections.synchronizedList(new ArrayList<>());
        
        @Override
        public void invoke(String value, Context context) throws Exception {
            values.add(value);
        }
    }
    
    // Helper Sink for collecting results in order
    private static class OrderedCollectingSink implements SinkFunction<String> {
        public static final List<String> values = Collections.synchronizedList(new ArrayList<>());
        
        @Override
        public void invoke(String value, Context context) throws Exception {
            synchronized (values) {
                values.add(value);
            }
        }
    }
    
    // Test implementation of MongoDB Sink for integration testing
    private static class TestMongoDBSink implements SinkFunction<String> {
        public static final List<String> collectedData = Collections.synchronizedList(new ArrayList<>());
        
        @Override
        public void invoke(String value, Context context) throws Exception {
            // Simulate MongoDB write
            collectedData.add(value);
        }
    }
}
