import React from 'react';
import { render, screen } from '@testing-library/react';
import MetricsChart from './MetricsChart';

// Mock Recharts to avoid rendering issues in tests
jest.mock('recharts', () => ({
  ResponsiveContainer: ({ children }) => <div data-testid="responsive-container">{children}</div>,
  LineChart: ({ children }) => <div data-testid="line-chart">{children}</div>,
  Line: () => <div data-testid="line" />,
  XAxis: () => <div data-testid="x-axis" />,
  YAxis: () => <div data-testid="y-axis" />,
  CartesianGrid: () => <div data-testid="cartesian-grid" />,
  Tooltip: () => <div data-testid="tooltip" />,
  Legend: () => <div data-testid="legend" />
}));

const mockMetrics = [
  {
    windowStart: 1704067200000, // 2024-01-01 00:00:00
    windowEnd: 1704070800000,
    count: 100,
    avg: 25.5,
    min: 10,
    max: 50
  },
  {
    windowStart: 1704070800000,
    windowEnd: 1704074400000,
    count: 150,
    avg: 30.0,
    min: 15,
    max: 60
  }
];

describe('MetricsChart Component', () => {
  test('renders without crashing', () => {
    render(<MetricsChart metrics={[]} />);
  });

  test('displays "No metrics data" message when metrics array is empty', () => {
    render(<MetricsChart metrics={[]} />);
    expect(screen.getByText(/No metrics data available/i)).toBeInTheDocument();
  });

  test('displays "No metrics data" message when metrics is null', () => {
    render(<MetricsChart metrics={null} />);
    expect(screen.getByText(/No metrics data available/i)).toBeInTheDocument();
  });

  test('renders summary statistics', () => {
    render(<MetricsChart metrics={mockMetrics} />);
    expect(screen.getByText('Total Events')).toBeInTheDocument();
    expect(screen.getByText('Average Value')).toBeInTheDocument();
    expect(screen.getByText('Windows')).toBeInTheDocument();
  });

  test('calculates total count correctly', () => {
    render(<MetricsChart metrics={mockMetrics} />);
    // 100 + 150 = 250
    expect(screen.getByText('250')).toBeInTheDocument();
  });

  test('calculates average value correctly', () => {
    render(<MetricsChart metrics={mockMetrics} />);
    // (25.5 + 30.0) / 2 = 27.75
    expect(screen.getByText('27.75')).toBeInTheDocument();
  });

  test('displays correct number of windows', () => {
    render(<MetricsChart metrics={mockMetrics} />);
    expect(screen.getByText('2')).toBeInTheDocument();
  });

  test('renders Recharts components when data is available', () => {
    render(<MetricsChart metrics={mockMetrics} />);
    expect(screen.getByTestId('responsive-container')).toBeInTheDocument();
    expect(screen.getByTestId('line-chart')).toBeInTheDocument();
  });

  test('renders chart axes', () => {
    render(<MetricsChart metrics={mockMetrics} />);
    expect(screen.getByTestId('x-axis')).toBeInTheDocument();
    expect(screen.getByTestId('y-axis')).toBeInTheDocument();
  });

  test('renders chart grid', () => {
    render(<MetricsChart metrics={mockMetrics} />);
    expect(screen.getByTestId('cartesian-grid')).toBeInTheDocument();
  });

  test('handles metrics with missing fields gracefully', () => {
    const incompleteMetrics = [
      { windowStart: Date.now() },
      { windowStart: Date.now(), count: 50 }
    ];
    render(<MetricsChart metrics={incompleteMetrics} />);
    expect(screen.getByText('Total Events')).toBeInTheDocument();
  });
});
