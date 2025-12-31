import React from 'react';
import { render, screen } from '@testing-library/react';
import StatsCards from './StatsCards';

const mockEvents = [
  { id: 'evt-1', userId: 'user-001', value: 10 },
  { id: 'evt-2', userId: 'user-002', value: 20 },
  { id: 'evt-3', userId: 'user-001', value: 30 }
];

const mockMetrics = [
  { count: 10 },
  { count: 20 }
];

describe('StatsCards Component', () => {
  test('renders without crashing', () => {
    render(<StatsCards events={[]} metrics={[]} />);
  });

  test('displays correct total events count', () => {
    render(<StatsCards events={mockEvents} metrics={[]} />);
    expect(screen.getByText('3')).toBeInTheDocument();
  });

  test('displays correct unique users count', () => {
    render(<StatsCards events={mockEvents} metrics={[]} />);
    // mockEvents has 2 unique users: user-001 and user-002
    expect(screen.getByText('2')).toBeInTheDocument();
  });

  test('calculates and displays average value correctly', () => {
    render(<StatsCards events={mockEvents} metrics={[]} />);
    // Average of 10, 20, 30 = 20.00
    expect(screen.getByText('20.00')).toBeInTheDocument();
  });

  test('displays correct metrics windows count', () => {
    render(<StatsCards events={[]} metrics={mockMetrics} />);
    expect(screen.getByText('2')).toBeInTheDocument();
  });

  test('renders all four stat card titles', () => {
    render(<StatsCards events={mockEvents} metrics={mockMetrics} />);
    expect(screen.getByText('Total Events')).toBeInTheDocument();
    expect(screen.getByText('Unique Users')).toBeInTheDocument();
    expect(screen.getByText('Avg Value')).toBeInTheDocument();
    expect(screen.getByText('Metrics Windows')).toBeInTheDocument();
  });

  test('handles empty events array', () => {
    render(<StatsCards events={[]} metrics={[]} />);
    const zeros = screen.getAllByText('0');
    expect(zeros.length).toBeGreaterThan(0);
    expect(screen.getByText('0.00')).toBeInTheDocument();
  });

  test('handles null events prop', () => {
    render(<StatsCards events={null} metrics={null} />);
    const zeros = screen.getAllByText('0');
    expect(zeros.length).toBeGreaterThan(0);
  });

  test('handles undefined props', () => {
    render(<StatsCards />);
    const zeros = screen.getAllByText('0');
    expect(zeros.length).toBeGreaterThan(0);
    expect(screen.getByText('0.00')).toBeInTheDocument();
  });

  test('renders four stat cards', () => {
    const { container } = render(<StatsCards events={mockEvents} metrics={mockMetrics} />);
    const cards = container.querySelectorAll('div[style*="border-top"]');
    expect(cards.length).toBeGreaterThanOrEqual(4);
  });
});
