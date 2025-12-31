import React from 'react';

/**
 * StatsCards Component
 * Display summary statistics in card format
 */
const StatsCards = ({ events, metrics }) => {
    const totalEvents = events?.length || 0;
    const totalMetrics = metrics?.length || 0;
    const avgValue = events?.length > 0 
        ? events.reduce((sum, e) => sum + (e.value || 0), 0) / events.length 
        : 0;
    const uniqueUsers = events?.length > 0 
        ? new Set(events.map(e => e.userId)).size 
        : 0;

    const cards = [
        { title: 'Total Events', value: totalEvents, color: '#3f51b5' },
        { title: 'Unique Users', value: uniqueUsers, color: '#4caf50' },
        { title: 'Avg Value', value: avgValue.toFixed(2), color: '#ff9800' },
        { title: 'Metrics Windows', value: totalMetrics, color: '#f44336' }
    ];

    return (
        <div style={styles.container}>
            {cards.map((card, index) => (
                <div key={index} style={{...styles.card, borderTopColor: card.color}}>
                    <h3 style={styles.value}>{card.value}</h3>
                    <p style={styles.title}>{card.title}</p>
                </div>
            ))}
        </div>
    );
};

const styles = {
    container: {
        display: 'grid',
        gridTemplateColumns: 'repeat(4, 1fr)',
        gap: '20px',
        marginBottom: '20px'
    },
    card: {
        backgroundColor: 'white',
        padding: '20px',
        borderRadius: '8px',
        boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
        borderTop: '4px solid',
        textAlign: 'center'
    },
    value: {
        fontSize: '32px',
        margin: '0 0 10px 0',
        fontWeight: 'bold'
    },
    title: {
        margin: 0,
        color: '#666',
        fontSize: '14px'
    }
};

export default StatsCards;
