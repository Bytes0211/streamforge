import React from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import { format } from 'date-fns';

/**
 * MetricsChart Component
 * Time-series visualization using Recharts
 */
const MetricsChart = ({ metrics }) => {
    if (!metrics || metrics.length === 0) {
        return <p style={styles.noData}>No metrics data available</p>;
    }

    // Transform metrics data for Recharts
    const chartData = metrics
        .sort((a, b) => a.windowStart - b.windowStart)
        .map(m => ({
            time: format(new Date(m.windowStart), 'HH:mm'),
            fullTime: format(new Date(m.windowStart), 'yyyy-MM-dd HH:mm:ss'),
            count: m.count || 0,
            avg: m.avg ? parseFloat(m.avg.toFixed(2)) : 0,
            min: m.min || 0,
            max: m.max || 0
        }));

    // Calculate summary stats
    const totalCount = metrics.reduce((sum, m) => sum + (m.count || 0), 0);
    const avgValue = metrics.reduce((sum, m) => sum + (m.avg || 0), 0) / metrics.length;

    return (
        <div style={styles.container}>
            <div style={styles.summary}>
                <div style={styles.stat}>
                    <h3>{totalCount}</h3>
                    <p>Total Events</p>
                </div>
                <div style={styles.stat}>
                    <h3>{avgValue.toFixed(2)}</h3>
                    <p>Average Value</p>
                </div>
                <div style={styles.stat}>
                    <h3>{metrics.length}</h3>
                    <p>Windows</p>
                </div>
            </div>
            
            <ResponsiveContainer width="100%" height={300}>
                <LineChart data={chartData} margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis 
                        dataKey="time" 
                        label={{ value: 'Time', position: 'insideBottom', offset: -5 }}
                    />
                    <YAxis 
                        label={{ value: 'Count', angle: -90, position: 'insideLeft' }}
                    />
                    <Tooltip 
                        content={<CustomTooltip />}
                    />
                    <Legend />
                    <Line 
                        type="monotone" 
                        dataKey="count" 
                        stroke="#3f51b5" 
                        strokeWidth={2}
                        name="Event Count"
                        dot={{ r: 3 }}
                    />
                    <Line 
                        type="monotone" 
                        dataKey="avg" 
                        stroke="#4caf50" 
                        strokeWidth={2}
                        name="Avg Value"
                        dot={{ r: 3 }}
                    />
                </LineChart>
            </ResponsiveContainer>
        </div>
    );
};

// Custom tooltip for better data display
const CustomTooltip = ({ active, payload, label }) => {
    if (active && payload && payload.length) {
        return (
            <div style={styles.tooltip}>
                <p style={styles.tooltipLabel}>{payload[0]?.payload?.fullTime}</p>
                {payload.map((entry, index) => (
                    <p key={index} style={{ color: entry.color }}>
                        {entry.name}: {entry.value}
                    </p>
                ))}
            </div>
        );
    }
    return null;
};

const styles = {
    container: {
        padding: '20px'
    },
    summary: {
        display: 'flex',
        justifyContent: 'space-around',
        marginBottom: '20px'
    },
    stat: {
        textAlign: 'center'
    },
    noData: {
        textAlign: 'center',
        color: '#666',
        padding: '40px'
    },
    tooltip: {
        backgroundColor: 'white',
        padding: '10px',
        border: '1px solid #ccc',
        borderRadius: '4px',
        boxShadow: '0 2px 4px rgba(0,0,0,0.1)'
    },
    tooltipLabel: {
        fontWeight: 'bold',
        marginBottom: '5px'
    }
};

export default MetricsChart;
