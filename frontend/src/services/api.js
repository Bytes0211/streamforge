import { DynamoDBClient, QueryCommand, ScanCommand } from '@aws-sdk/client-dynamodb';
import { unmarshall } from '@aws-sdk/util-dynamodb';

// Check if we should use mock data
const USE_MOCK_DATA = process.env.REACT_APP_USE_MOCK_DATA === 'true';

// Initialize DynamoDB client
const client = new DynamoDBClient({ 
    region: process.env.REACT_APP_AWS_REGION || 'us-east-1'
});

// Mock data generators
const generateMockEvents = (count = 50) => {
    const eventTypes = ['click', 'view', 'purchase', 'signup', 'logout'];
    const users = ['user-001', 'user-002', 'user-003', 'user-004', 'user-005'];
    
    return Array.from({ length: count }, (_, i) => ({
        id: `evt-${Date.now()}-${i}`,
        type: eventTypes[Math.floor(Math.random() * eventTypes.length)],
        userId: users[Math.floor(Math.random() * users.length)],
        value: parseFloat((Math.random() * 100).toFixed(2)),
        timestamp: Date.now() - (i * 60000), // 1 minute apart
        payload: `mock-payload-${i}`,
        processedAt: Date.now()
    }));
};

const generateMockMetrics = (count = 24) => {
    return Array.from({ length: count }, (_, i) => {
        const windowStart = Date.now() - (i * 3600000); // 1 hour apart
        const eventCount = Math.floor(Math.random() * 100) + 20;
        const avgVal = parseFloat((Math.random() * 50).toFixed(2));
        
        return {
            userId: `user-00${(i % 5) + 1}`,
            eventType: ['click', 'view', 'purchase'][i % 3],
            windowStart,
            windowEnd: windowStart + 3600000,
            count: eventCount,
            sum: eventCount * avgVal,
            avg: avgVal,
            min: parseFloat((avgVal * 0.5).toFixed(2)),
            max: parseFloat((avgVal * 1.5).toFixed(2))
        };
    });
};

/**
 * Fetch recent events from DynamoDB
 * @param {number} limit - Maximum number of events to return
 * @returns {Promise<Array>} Array of event objects
 */
export const getRecentEvents = async (limit = 50) => {
    // Use mock data if configured
    if (USE_MOCK_DATA) {
        console.log('Using mock event data (REACT_APP_USE_MOCK_DATA=true)');
        return new Promise(resolve => {
            setTimeout(() => resolve(generateMockEvents(limit)), 500);
        });
    }
    
    try {
        const command = new ScanCommand({
            TableName: process.env.REACT_APP_DYNAMODB_TABLE_EVENTS || 'streamforge-dev-processed-data',
            Limit: limit
        });
        
        const response = await client.send(command);
        return response.Items?.map(item => unmarshall(item)) || [];
    } catch (error) {
        console.error('Error fetching events:', error);
        console.warn('Falling back to mock data due to error');
        return generateMockEvents(limit);
    }
};

/**
 * Query events by user ID
 * @param {string} userId - User identifier
 * @returns {Promise<Array>} Array of events for the user
 */
export const getEventsByUser = async (userId) => {
    try {
        const command = new QueryCommand({
            TableName: process.env.REACT_APP_DYNAMODB_TABLE_EVENTS || 'streamforge-dev-processed-data',
            IndexName: 'UserIdIndex',
            KeyConditionExpression: 'userId = :uid',
            ExpressionAttributeValues: {
                ':uid': { S: userId }
            }
        });
        
        const response = await client.send(command);
        return response.Items?.map(item => unmarshall(item)) || [];
    } catch (error) {
        console.error('Error fetching events by user:', error);
        throw error;
    }
};

/**
 * Query events by type
 * @param {string} eventType - Event type (click, view, etc.)
 * @returns {Promise<Array>} Array of events of the specified type
 */
export const getEventsByType = async (eventType) => {
    try {
        const command = new QueryCommand({
            TableName: process.env.REACT_APP_DYNAMODB_TABLE_EVENTS || 'streamforge-dev-processed-data',
            IndexName: 'TypeIndex',
            KeyConditionExpression: '#type = :etype',
            ExpressionAttributeNames: {
                '#type': 'type'
            },
            ExpressionAttributeValues: {
                ':etype': { S: eventType }
            }
        });
        
        const response = await client.send(command);
        return response.Items?.map(item => unmarshall(item)) || [];
    } catch (error) {
        console.error('Error fetching events by type:', error);
        throw error;
    }
};

/**
 * Fetch aggregated metrics within a time range
 * @param {number} startTime - Start timestamp (epoch milliseconds)
 * @param {number} endTime - End timestamp (epoch milliseconds)
 * @returns {Promise<Array>} Array of aggregated metric objects
 */
export const getMetrics = async (startTime, endTime) => {
    // Use mock data if configured
    if (USE_MOCK_DATA) {
        console.log('Using mock metrics data (REACT_APP_USE_MOCK_DATA=true)');
        return new Promise(resolve => {
            setTimeout(() => resolve(generateMockMetrics(24)), 500);
        });
    }
    
    try {
        const command = new ScanCommand({
            TableName: process.env.REACT_APP_DYNAMODB_TABLE_METRICS || 'streamforge-dev-aggregated-metrics',
            FilterExpression: 'windowStart BETWEEN :start AND :end',
            ExpressionAttributeValues: {
                ':start': { N: startTime.toString() },
                ':end': { N: endTime.toString() }
            }
        });
        
        const response = await client.send(command);
        return response.Items?.map(item => unmarshall(item)) || [];
    } catch (error) {
        console.error('Error fetching metrics:', error);
        console.warn('Falling back to mock data due to error');
        return generateMockMetrics(24);
    }
};

/**
 * Get metrics for a specific user
 * @param {string} userId - User identifier
 * @param {number} limit - Maximum number of metric windows to return
 * @returns {Promise<Array>} Array of metric objects for the user
 */
export const getMetricsByUser = async (userId, limit = 100) => {
    try {
        const command = new QueryCommand({
            TableName: process.env.REACT_APP_DYNAMODB_TABLE_METRICS || 'streamforge-dev-aggregated-metrics',
            KeyConditionExpression: 'userId = :uid',
            ExpressionAttributeValues: {
                ':uid': { S: userId }
            },
            Limit: limit,
            ScanIndexForward: false // Sort descending (most recent first)
        });
        
        const response = await client.send(command);
        return response.Items?.map(item => unmarshall(item)) || [];
    } catch (error) {
        console.error('Error fetching metrics by user:', error);
        throw error;
    }
};

const api = {
    getRecentEvents,
    getEventsByUser,
    getEventsByType,
    getMetrics,
    getMetricsByUser
};

export default api;
