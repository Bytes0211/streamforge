import React from 'react';
import { render, screen } from '@testing-library/react';
import EventList from './EventList';

const mockEvents = [
  {
    id: 'evt-1',
    type: 'click',
    userId: 'user-001',
    value: 10.5,
    timestamp: 1704067200000 // 2024-01-01 00:00:00
  },
  {
    id: 'evt-2',
    type: 'view',
    userId: 'user-002',
    value: 20.75,
    timestamp: 1704067200000
  }
];

describe('EventList Component', () => {
  test('renders without crashing', () => {
    render(<EventList events={[]} />);
  });

  test('displays "No events" message when events array is empty', () => {
    render(<EventList events={[]} />);
    expect(screen.getByText(/No events to display/i)).toBeInTheDocument();
  });

  test('renders table headers', () => {
    render(<EventList events={mockEvents} />);
    expect(screen.getByText('ID')).toBeInTheDocument();
    expect(screen.getByText('Type')).toBeInTheDocument();
    expect(screen.getByText('User ID')).toBeInTheDocument();
    expect(screen.getByText('Value')).toBeInTheDocument();
    expect(screen.getByText('Timestamp')).toBeInTheDocument();
  });

  test('renders event data correctly', () => {
    render(<EventList events={mockEvents} />);
    
    // Check for event IDs
    expect(screen.getByText('evt-1')).toBeInTheDocument();
    expect(screen.getByText('evt-2')).toBeInTheDocument();
    
    // Check for event types
    expect(screen.getByText('click')).toBeInTheDocument();
    expect(screen.getByText('view')).toBeInTheDocument();
    
    // Check for user IDs
    expect(screen.getByText('user-001')).toBeInTheDocument();
    expect(screen.getByText('user-002')).toBeInTheDocument();
  });

  test('formats values with 2 decimal places', () => {
    render(<EventList events={mockEvents} />);
    expect(screen.getByText('10.50')).toBeInTheDocument();
    expect(screen.getByText('20.75')).toBeInTheDocument();
  });

  test('renders correct number of event rows', () => {
    const { container } = render(<EventList events={mockEvents} />);
    const rows = container.querySelectorAll('tbody tr');
    expect(rows).toHaveLength(2);
  });

  test('handles null events prop gracefully', () => {
    render(<EventList events={null} />);
    expect(screen.getByText(/No events to display/i)).toBeInTheDocument();
  });

  test('handles undefined events prop gracefully', () => {
    render(<EventList events={undefined} />);
    expect(screen.getByText(/No events to display/i)).toBeInTheDocument();
  });
});
