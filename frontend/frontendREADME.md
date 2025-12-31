# StreamForge React Frontend

A real-time analytics dashboard for viewing processed events and aggregated metrics from the StreamForge streaming pipeline.

## Architecture

### Component Structure

```
frontend/
├── public/
│   └── index.html              # HTML template
├── src/
│   ├── components/
│   │   ├── Dashboard.jsx       # Main dashboard container
│   │   ├── EventList.jsx       # Table of recent events
│   │   ├── MetricsChart.jsx    # Time-series visualization
│   │   └── StatsCards.jsx      # Summary statistics cards
│   ├── services/
│   │   └── api.js              # DynamoDB API client
│   ├── App.jsx                 # Root application component
│   ├── index.jsx               # Application entry point
│   └── aws-exports.js          # AWS Amplify configuration
├── package.json                # Dependencies
└── README.md                   # This file
```

## Features

### Dashboard Components

1. **StatsCards** - Display key metrics:
   - Total events processed (24h)
   - Average processing time
   - Active users
   - Error rate

2. **EventList** - Recent events table:
   - Event ID, type, user ID, value, timestamp
   - Pagination (50 events per page)
   - Real-time updates (polling every 5s)

3. **MetricsChart** - Time-series visualization:
   - Event count over time (1-minute windows)
   - Average values by event type
   - Interactive tooltips with drill-down

4. **Dashboard** - Main container:
   - Layout management
   - Data fetching orchestration
   - Error handling and loading states

## Technology Stack

- **React 18**: UI framework
- **AWS SDK v3**: DynamoDB client
- **AWS Amplify**: Authentication and hosting
- **Recharts**: Data visualization
- **date-fns**: Date formatting
- **Material-UI** (optional): Component library

## Prerequisites

- **Node.js 18+** and **npm**
- **AWS credentials** (optional - for DynamoDB integration)
- **DynamoDB tables** deployed (optional - see AWS_DEPLOYMENT.md)

## Quick Start

### 1. Install Dependencies

```bash
cd frontend
npm install
```

### 2. Configure Environment

The frontend comes with `.env.local` pre-configured for **local development with mock data**.

**For local development (no AWS required):**

The default `.env.local` uses mock data:
```bash
REACT_APP_AWS_REGION=us-east-1
REACT_APP_DYNAMODB_TABLE_EVENTS=streamforge-dev-processed-data
REACT_APP_DYNAMODB_TABLE_METRICS=streamforge-dev-aggregated-metrics
REACT_APP_USE_MOCK_DATA=true  # Uses mock data instead of AWS
```

**For AWS DynamoDB integration:**

Edit `.env.local` and set:
```bash
REACT_APP_USE_MOCK_DATA=false
```

Ensure your AWS credentials are configured:
```bash
# Configure AWS CLI with your credentials
aws configure
```

### 3. Start Development Server

```bash
npm start
```

The application will open at **http://localhost:3000**

### 4. Build for Production

```bash
npm run build
```

The optimized build will be in the `build/` directory.

## Configuration Details

### Environment Variables

See `.env.example` for all available configuration options:

- `REACT_APP_AWS_REGION` - AWS region (default: us-east-1)
- `REACT_APP_DYNAMODB_TABLE_EVENTS` - Events table name
- `REACT_APP_DYNAMODB_TABLE_METRICS` - Metrics table name
- `REACT_APP_USE_MOCK_DATA` - Use mock data instead of AWS (true/false)

### AWS Amplify Configuration

The `src/aws-exports.js` file is auto-generated and uses environment variables. No manual editing needed.

## Development Modes

### Mode 1: Local Development (Mock Data)

**Best for:** UI development, testing without AWS infrastructure

```bash
# .env.local
REACT_APP_USE_MOCK_DATA=true
```

- Uses generated mock data
- No AWS credentials required
- Simulates realistic event and metrics data
- Automatic fallback if AWS calls fail

### Mode 2: AWS Integration

**Best for:** Testing with real DynamoDB data

```bash
# .env.local
REACT_APP_USE_MOCK_DATA=false
```

**Requirements:**
- AWS credentials configured (`~/.aws/credentials`)
- DynamoDB tables deployed via Terraform
- IAM permissions for DynamoDB read operations

### Component Development

#### Example: EventList Component

```jsx
import React, { useState, useEffect } from 'react';
import { DynamoDBClient, ScanCommand } from '@aws-sdk/client-dynamodb';
import { unmarshall } from '@aws-sdk/util-dynamodb';

const EventList = () => {
    const [events, setEvents] = useState([]);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        const fetchEvents = async () => {
            const client = new DynamoDBClient({ region: 'us-east-1' });
            const command = new ScanCommand({
                TableName: 'streamforge-dev-processed-data',
                Limit: 50
            });
            
            const response = await client.send(command);
            const items = response.Items.map(item => unmarshall(item));
            setEvents(items);
            setLoading(false);
        };

        fetchEvents();
        const interval = setInterval(fetchEvents, 5000); // Poll every 5s
        
        return () => clearInterval(interval);
    }, []);

    if (loading) return <div>Loading...</div>;

    return (
        <table>
            <thead>
                <tr>
                    <th>ID</th>
                    <th>Type</th>
                    <th>User</th>
                    <th>Value</th>
                    <th>Timestamp</th>
                </tr>
            </thead>
            <tbody>
                {events.map(event => (
                    <tr key={event.id}>
                        <td>{event.id}</td>
                        <td>{event.type}</td>
                        <td>{event.userId}</td>
                        <td>{event.value}</td>
                        <td>{new Date(event.timestamp).toLocaleString()}</td>
                    </tr>
                ))}
            </tbody>
        </table>
    );
};

export default EventList;
```

#### Example: API Service

```javascript
// src/services/api.js
import { DynamoDBClient, QueryCommand, ScanCommand } from '@aws-sdk/client-dynamodb';
import { unmarshall } from '@aws-sdk/util-dynamodb';

const client = new DynamoDBClient({ region: process.env.REACT_APP_AWS_REGION });

export const getRecentEvents = async (limit = 50) => {
    const command = new ScanCommand({
        TableName: process.env.REACT_APP_DYNAMODB_TABLE_EVENTS,
        Limit: limit
    });
    
    const response = await client.send(command);
    return response.Items.map(item => unmarshall(item));
};

export const getEventsByUser = async (userId) => {
    const command = new QueryCommand({
        TableName: process.env.REACT_APP_DYNAMODB_TABLE_EVENTS,
        IndexName: 'UserIdIndex',
        KeyConditionExpression: 'userId = :uid',
        ExpressionAttributeValues: {
            ':uid': { S: userId }
        }
    });
    
    const response = await client.send(command);
    return response.Items.map(item => unmarshall(item));
};

export const getMetrics = async (startTime, endTime) => {
    const command = new ScanCommand({
        TableName: process.env.REACT_APP_DYNAMODB_TABLE_METRICS,
        FilterExpression: 'windowStart BETWEEN :start AND :end',
        ExpressionAttributeValues: {
            ':start': { N: startTime.toString() },
            ':end': { N: endTime.toString() }
        }
    });
    
    const response = await client.send(command);
    return response.Items.map(item => unmarshall(item));
};
```

## Deployment

### Option 1: AWS Amplify (Recommended)

```bash
# Install Amplify CLI
npm install -g @aws-amplify/cli

# Initialize Amplify
amplify init

# Add hosting
amplify add hosting

# Build and deploy
npm run build
amplify publish
```

### Option 2: S3 + CloudFront

```bash
# Build production bundle
npm run build

# Sync to S3
aws s3 sync build/ s3://streamforge-frontend-bucket

# Invalidate CloudFront cache
aws cloudfront create-invalidation \
  --distribution-id EXXXXXXXXX \
  --paths "/*"
```

## Testing

```bash
# Run unit tests
npm test

# Run with coverage
npm test -- --coverage

# Run e2e tests (if configured)
npm run test:e2e
```

## Performance Optimization

1. **Code Splitting**: Use React.lazy() for route-based code splitting
2. **Memoization**: Use React.memo() for expensive components
3. **Pagination**: Limit DynamoDB queries to 50 items
4. **Caching**: Implement client-side caching with React Query or SWR
5. **Polling**: Use WebSockets for real-time updates instead of polling

## Security

1. **Authentication**: Use AWS Amplify Auth with Cognito
2. **Authorization**: Implement IAM policies for DynamoDB access
3. **API Keys**: Never commit credentials to version control
4. **CORS**: Configure CORS on API Gateway if using REST API
5. **HTTPS**: Always use HTTPS in production (enforced by Amplify)

## Directory Structure Details

### `src/components/`

- **Dashboard.jsx**: Main container with layout, manages state for child components
- **EventList.jsx**: Displays recent events in table format with pagination
- **MetricsChart.jsx**: Recharts-based time-series visualization
- **StatsCards.jsx**: Grid of summary statistic cards (total events, avg value, etc.)

### `src/services/`

- **api.js**: Centralized API client with DynamoDB operations
  - `getRecentEvents(limit)`: Fetch recent events
  - `getEventsByUser(userId)`: Query events by user
  - `getMetrics(startTime, endTime)`: Fetch aggregated metrics

### `public/`

- **index.html**: HTML template with root div
- **manifest.json**: PWA configuration (optional)
- **favicon.ico**: Site icon

## Dependencies

```json
{
  "dependencies": {
    "react": "^18.2.0",
    "react-dom": "^18.2.0",
    "@aws-sdk/client-dynamodb": "^3.400.0",
    "@aws-sdk/lib-dynamodb": "^3.400.0",
    "@aws-sdk/util-dynamodb": "^3.400.0",
    "aws-amplify": "^5.3.0",
    "@aws-amplify/ui-react": "^5.0.0",
    "recharts": "^2.8.0",
    "date-fns": "^2.30.0"
  },
  "devDependencies": {
    "@testing-library/react": "^14.0.0",
    "@testing-library/jest-dom": "^6.0.0"
  }
}
```

## Troubleshooting

### DynamoDB Access Denied

**Error**: `AccessDeniedException: User is not authorized to perform: dynamodb:Scan`

**Solution**: Ensure IAM role/user has DynamoDB read permissions:
```json
{
  "Effect": "Allow",
  "Action": [
    "dynamodb:Scan",
    "dynamodb:Query",
    "dynamodb:GetItem"
  ],
  "Resource": "arn:aws:dynamodb:us-east-1:*:table/streamforge-*"
}
```

### CORS Errors

**Error**: `No 'Access-Control-Allow-Origin' header`

**Solution**: Configure CORS on API Gateway or use Amplify API which handles CORS automatically.

### Slow Loading

**Issue**: Dashboard takes >5 seconds to load

**Solutions**:
- Reduce DynamoDB scan limit from 50 to 25
- Implement pagination with LastEvaluatedKey
- Use DynamoDB GSI for efficient queries
- Add React.memo() to heavy components

## Future Enhancements

1. **Real-time Updates**: Replace polling with WebSockets (AWS AppSync)
2. **User Authentication**: Add Cognito user pools
3. **Advanced Filtering**: Date range picker, event type filters
4. **Export Data**: Download CSV/JSON exports
5. **Dark Mode**: Theme toggle with localStorage persistence
6. **Mobile Responsive**: Optimize layout for mobile devices

## Related Documentation

- [AWS Deployment Guide](../docs/AWS_DEPLOYMENT.md)
- [DynamoDB Schema](../docs/mongodb-schema.md)
- [Project Status](../project_status.md)

## License

MIT
