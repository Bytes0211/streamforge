import React, { useState, useEffect } from 'react';
import EventList from './EventList';
import MetricsChart from './MetricsChart';
import StatsCards from './StatsCards';
import { getRecentEvents, getMetrics } from '../services/api';

/**
 * Main Dashboard Component
 * Orchestrates data fetching and layout for StreamForge analytics
 */
const Dashboard = () => {
    const [events, setEvents] = useState([]);
    const [metrics, setMetrics] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => {
        const fetchData = async () => {
            try {
                setLoading(true);
                
                // Fetch recent events
                const eventsData = await getRecentEvents(50);
                setEvents(eventsData);

                // Fetch metrics for last 24 hours
                const now = Date.now();
                const yesterday = now - (24 * 60 * 60 * 1000);
                const metricsData = await getMetrics(yesterday, now);
                setMetrics(metricsData);

                setLoading(false);
            } catch (err) {
                console.error('Error fetching data:', err);
                setError(err.message);
                setLoading(false);
            }
        };

        fetchData();
        
        // Refresh data every 30 seconds
        const interval = setInterval(fetchData, 30000);
        
        return () => clearInterval(interval);
    }, []);

    if (loading) {
        return (
            <div style={styles.loading}>
                <h2>Loading StreamForge Dashboard...</h2>
            </div>
        );
    }

    if (error) {
        return (
            <div style={styles.error}>
                <h2>Error Loading Dashboard</h2>
                <p>{error}</p>
            </div>
        );
    }

    return (
        <div style={styles.container}>
            <header style={styles.header}>
                <h1>StreamForge Analytics Dashboard</h1>
                <p>Real-time event processing and metrics</p>
            </header>

            <StatsCards events={events} metrics={metrics} />
            
            <div style={styles.grid}>
                <div style={styles.chartSection}>
                    <h2>Event Metrics (24h)</h2>
                    <MetricsChart metrics={metrics} />
                </div>

                <div style={styles.eventsSection}>
                    <h2>Recent Events</h2>
                    <EventList events={events} />
                </div>
            </div>
        </div>
    );
};

const styles = {
    container: {
        padding: '20px',
        fontFamily: 'Arial, sans-serif',
        backgroundColor: '#f5f5f5',
        minHeight: '100vh'
    },
    header: {
        backgroundColor: '#282c34',
        color: 'white',
        padding: '20px',
        borderRadius: '8px',
        marginBottom: '20px'
    },
    grid: {
        display: 'grid',
        gridTemplateColumns: '1fr 1fr',
        gap: '20px',
        marginTop: '20px'
    },
    chartSection: {
        backgroundColor: 'white',
        padding: '20px',
        borderRadius: '8px',
        boxShadow: '0 2px 4px rgba(0,0,0,0.1)'
    },
    eventsSection: {
        backgroundColor: 'white',
        padding: '20px',
        borderRadius: '8px',
        boxShadow: '0 2px 4px rgba(0,0,0,0.1)'
    },
    loading: {
        display: 'flex',
        justifyContent: 'center',
        alignItems: 'center',
        height: '100vh',
        backgroundColor: '#f5f5f5'
    },
    error: {
        display: 'flex',
        flexDirection: 'column',
        justifyContent: 'center',
        alignItems: 'center',
        height: '100vh',
        backgroundColor: '#f5f5f5',
        color: '#d32f2f'
    }
};

export default Dashboard;
