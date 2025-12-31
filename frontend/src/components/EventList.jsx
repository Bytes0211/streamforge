import React from 'react';
import { format } from 'date-fns';

/**
 * EventList Component
 * Displays a table of recent events with pagination
 */
const EventList = ({ events }) => {
    if (!events || events.length === 0) {
        return <p>No events to display</p>;
    }

    return (
        <div style={styles.container}>
            <table style={styles.table}>
                <thead>
                    <tr style={styles.headerRow}>
                        <th style={styles.th}>ID</th>
                        <th style={styles.th}>Type</th>
                        <th style={styles.th}>User ID</th>
                        <th style={styles.th}>Value</th>
                        <th style={styles.th}>Timestamp</th>
                    </tr>
                </thead>
                <tbody>
                    {events.map((event, index) => (
                        <tr key={event.id || index} style={styles.row}>
                            <td style={styles.td}>{event.id}</td>
                            <td style={styles.td}>{event.type}</td>
                            <td style={styles.td}>{event.userId}</td>
                            <td style={styles.td}>{event.value?.toFixed(2)}</td>
                            <td style={styles.td}>
                                {format(new Date(event.timestamp), 'yyyy-MM-dd HH:mm:ss')}
                            </td>
                        </tr>
                    ))}
                </tbody>
            </table>
        </div>
    );
};

const styles = {
    container: {
        overflowX: 'auto'
    },
    table: {
        width: '100%',
        borderCollapse: 'collapse',
        fontSize: '14px'
    },
    headerRow: {
        backgroundColor: '#f0f0f0',
        borderBottom: '2px solid #ddd'
    },
    th: {
        padding: '12px 8px',
        textAlign: 'left',
        fontWeight: 'bold'
    },
    row: {
        borderBottom: '1px solid #eee'
    },
    td: {
        padding: '10px 8px'
    }
};

export default EventList;
