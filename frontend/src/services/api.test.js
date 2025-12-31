import { getRecentEvents, getEventsByUser, getMetrics, getMetricsByUser } from './api';

// Mock AWS SDK
jest.mock('@aws-sdk/client-dynamodb', () => ({
  DynamoDBClient: jest.fn(() => ({
    send: jest.fn()
  })),
  ScanCommand: jest.fn(),
  QueryCommand: jest.fn()
}));

jest.mock('@aws-sdk/util-dynamodb', () => ({
  unmarshall: jest.fn(item => item)
}));

// Set mock data mode for tests
process.env.REACT_APP_USE_MOCK_DATA = 'true';

describe('API Service', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe('getRecentEvents', () => {
    test('returns array of events in mock mode', async () => {
      const events = await getRecentEvents(50);
      expect(Array.isArray(events)).toBe(true);
      expect(events.length).toBeGreaterThan(0);
      expect(events.length).toBeLessThanOrEqual(50);
    });

    test('events have required fields', async () => {
      const events = await getRecentEvents(10);
      const event = events[0];
      
      expect(event).toHaveProperty('id');
      expect(event).toHaveProperty('type');
      expect(event).toHaveProperty('userId');
      expect(event).toHaveProperty('value');
      expect(event).toHaveProperty('timestamp');
      expect(event).toHaveProperty('payload');
    });

    test('respects limit parameter', async () => {
      const events = await getRecentEvents(5);
      expect(events.length).toBeLessThanOrEqual(5);
    });

    test('generates different event types', async () => {
      const events = await getRecentEvents(50);
      const eventTypes = new Set(events.map(e => e.type));
      expect(eventTypes.size).toBeGreaterThan(1);
    });
  });

  describe('getMetrics', () => {
    test('returns array of metrics in mock mode', async () => {
      const startTime = Date.now() - 86400000; // 24 hours ago
      const endTime = Date.now();
      
      const metrics = await getMetrics(startTime, endTime);
      expect(Array.isArray(metrics)).toBe(true);
      expect(metrics.length).toBeGreaterThan(0);
    });

    test('metrics have required fields', async () => {
      const startTime = Date.now() - 86400000;
      const endTime = Date.now();
      
      const metrics = await getMetrics(startTime, endTime);
      const metric = metrics[0];
      
      expect(metric).toHaveProperty('userId');
      expect(metric).toHaveProperty('eventType');
      expect(metric).toHaveProperty('windowStart');
      expect(metric).toHaveProperty('windowEnd');
      expect(metric).toHaveProperty('count');
      expect(metric).toHaveProperty('sum');
      expect(metric).toHaveProperty('avg');
      expect(metric).toHaveProperty('min');
      expect(metric).toHaveProperty('max');
    });

    test('metric values are valid numbers', async () => {
      const startTime = Date.now() - 86400000;
      const endTime = Date.now();
      
      const metrics = await getMetrics(startTime, endTime);
      const metric = metrics[0];
      
      expect(typeof metric.count).toBe('number');
      expect(typeof metric.avg).toBe('number');
      expect(typeof metric.sum).toBe('number');
      expect(metric.count).toBeGreaterThan(0);
    });

    test('min value is less than or equal to avg', async () => {
      const startTime = Date.now() - 86400000;
      const endTime = Date.now();
      
      const metrics = await getMetrics(startTime, endTime);
      const metric = metrics[0];
      
      expect(metric.min).toBeLessThanOrEqual(metric.avg);
    });

    test('max value is greater than or equal to avg', async () => {
      const startTime = Date.now() - 86400000;
      const endTime = Date.now();
      
      const metrics = await getMetrics(startTime, endTime);
      const metric = metrics[0];
      
      expect(metric.max).toBeGreaterThanOrEqual(metric.avg);
    });
  });

  describe('Mock Data Generation', () => {
    test('generates realistic event timestamps', async () => {
      const events = await getRecentEvents(10);
      const timestamps = events.map(e => e.timestamp);
      
      // All timestamps should be valid
      timestamps.forEach(ts => {
        expect(ts).toBeGreaterThan(0);
        expect(ts).toBeLessThanOrEqual(Date.now());
      });
    });

    test('generates events with different user IDs', async () => {
      const events = await getRecentEvents(50);
      const userIds = new Set(events.map(e => e.userId));
      expect(userIds.size).toBeGreaterThan(1);
    });

    test('generates valid numeric values', async () => {
      const events = await getRecentEvents(50);
      events.forEach(event => {
        expect(typeof event.value).toBe('number');
        expect(event.value).toBeGreaterThan(0);
        expect(event.value).toBeLessThan(100);
      });
    });
  });
});
