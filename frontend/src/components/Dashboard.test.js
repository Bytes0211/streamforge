import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import Dashboard from './Dashboard';
import * as api from '../services/api';

// Mock the API service
jest.mock('../services/api');

// Mock child components to avoid Recharts issues
jest.mock('./EventList', () => {
  return function MockEventList({ events }) {
    return <div data-testid="event-list">EventList with {events?.length || 0} events</div>;
  };
});

jest.mock('./MetricsChart', () => {
  return function MockMetricsChart({ metrics }) {
    return <div data-testid="metrics-chart">MetricsChart with {metrics?.length || 0} metrics</div>;
  };
});

jest.mock('./StatsCards', () => {
  return function MockStatsCards({ events, metrics }) {
    return <div data-testid="stats-cards">StatsCards</div>;
  };
});

const mockEvents = [
  {
    id: 'evt-1',
    type: 'click',
    userId: 'user-001',
    value: 10.5,
    timestamp: Date.now(),
    payload: 'test'
  },
  {
    id: 'evt-2',
    type: 'view',
    userId: 'user-002',
    value: 20.0,
    timestamp: Date.now(),
    payload: 'test'
  }
];

const mockMetrics = [
  {
    userId: 'user-001',
    eventType: 'click',
    windowStart: Date.now() - 3600000,
    windowEnd: Date.now(),
    count: 10,
    sum: 100,
    avg: 10,
    min: 5,
    max: 15
  }
];

describe('Dashboard Component', () => {
  beforeEach(() => {
    // Reset mocks before each test
    jest.clearAllMocks();
    api.getRecentEvents.mockResolvedValue(mockEvents);
    api.getMetrics.mockResolvedValue(mockMetrics);
  });

  test('renders loading state initially', () => {
    render(<Dashboard />);
    expect(screen.getByText(/Loading StreamForge Dashboard/i)).toBeInTheDocument();
  });

  test('renders dashboard after data loads', async () => {
    render(<Dashboard />);
    
    await waitFor(() => {
      expect(screen.getByText(/StreamForge Analytics Dashboard/i)).toBeInTheDocument();
    });
  });

  test('renders Recent Events section', async () => {
    render(<Dashboard />);
    
    await waitFor(() => {
      expect(screen.getByText(/Recent Events/i)).toBeInTheDocument();
    });
  });

  test('renders Event Metrics section', async () => {
    render(<Dashboard />);
    
    await waitFor(() => {
      expect(screen.getByText(/Event Metrics/i)).toBeInTheDocument();
    });
  });

  test('displays error message when API fails', async () => {
    // Suppress console.error for this test
    const consoleSpy = jest.spyOn(console, 'error').mockImplementation(() => {});
    
    api.getRecentEvents.mockRejectedValue(new Error('API Error'));
    
    render(<Dashboard />);
    
    await waitFor(() => {
      expect(screen.getByText(/Error Loading Dashboard/i)).toBeInTheDocument();
    });
    
    consoleSpy.mockRestore();
  });

  test('calls API with correct parameters', async () => {
    render(<Dashboard />);
    
    await waitFor(() => {
      expect(api.getRecentEvents).toHaveBeenCalledWith(50);
      expect(api.getMetrics).toHaveBeenCalled();
    });
  });
});
