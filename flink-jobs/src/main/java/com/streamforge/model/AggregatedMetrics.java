package com.streamforge.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * Represents aggregated metrics from windowed computations
 */
public class AggregatedMetrics implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    private final String userId;
    private final String eventType;
    private final long count;
    private final double sum;
    private final double avg;
    private final double min;
    private final double max;
    private final long windowStart;
    private final long windowEnd;
    
    public AggregatedMetrics(String userId, String eventType, long count, 
                            double sum, double avg, double min, double max,
                            long windowStart, long windowEnd) {
        this.userId = userId;
        this.eventType = eventType;
        this.count = count;
        this.sum = sum;
        this.avg = avg;
        this.min = min;
        this.max = max;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }
    
    public String getUserId() {
        return userId;
    }
    
    public String getEventType() {
        return eventType;
    }
    
    public long getCount() {
        return count;
    }
    
    public double getSum() {
        return sum;
    }
    
    public double getAvg() {
        return avg;
    }
    
    public double getMin() {
        return min;
    }
    
    public double getMax() {
        return max;
    }
    
    public long getWindowStart() {
        return windowStart;
    }
    
    public long getWindowEnd() {
        return windowEnd;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggregatedMetrics that = (AggregatedMetrics) o;
        return count == that.count &&
               Double.compare(that.sum, sum) == 0 &&
               Double.compare(that.avg, avg) == 0 &&
               Double.compare(that.min, min) == 0 &&
               Double.compare(that.max, max) == 0 &&
               windowStart == that.windowStart &&
               windowEnd == that.windowEnd &&
               Objects.equals(userId, that.userId) &&
               Objects.equals(eventType, that.eventType);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(userId, eventType, count, sum, avg, min, max, windowStart, windowEnd);
    }
    
    @Override
    public String toString() {
        return "AggregatedMetrics{" +
               "userId='" + userId + '\'' +
               ", eventType='" + eventType + '\'' +
               ", count=" + count +
               ", sum=" + sum +
               ", avg=" + avg +
               ", min=" + min +
               ", max=" + max +
               ", windowStart=" + windowStart +
               ", windowEnd=" + windowEnd +
               '}';
    }
}
