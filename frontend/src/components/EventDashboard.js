// src/components/EventDashboard.js
import React, { useState, useEffect } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend } from 'recharts';

function EventDashboard() {
  const [events, setEvents] = useState([]);
  const [metrics, setMetrics] = useState([]);
  
  useEffect(() => {
    // Fetch events from DynamoDB via API Gateway
    fetchEvents();
    fetchMetrics();
    
    // Poll every 5 seconds
    const interval = setInterval(() => {
      fetchEvents();
      fetchMetrics();
    }, 5000);
    
    return () => clearInterval(interval);
  }, []);
  
  const fetchEvents = async () => {
    // Implementation would query DynamoDB via API Gateway
    console.log('Fetching events...');
  };
  
  const fetchMetrics = async () => {
    // Implementation would query aggregated metrics
    console.log('Fetching metrics...');
  };
  
  return (
    <div className="dashboard">
      <h1>StreamForge Real-Time Dashboard</h1>
      
      <div className="metrics-summary">
        <div className="metric-card">
          <h3>Total Events</h3>
          <p>{events.length}</p>
        </div>
        <div className="metric-card">
          <h3>Active Users</h3>
          <p>{new Set(events.map(e => e.userId)).size}</p>
        </div>
      </div>
      
      <div className="chart-container">
        <h2>Events Over Time</h2>
        <LineChart width={800} height={400} data={metrics}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="timestamp" />
          <YAxis />
          <Tooltip />
          <Legend />
          <Line type="monotone" dataKey="count" stroke="#8884d8" />
        </LineChart>
      </div>
      
      <div className="events-table">
        <h2>Recent Events</h2>
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
            {events.slice(0, 10).map(event => (
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
      </div>
    </div>
  );
}

export default EventDashboard;
