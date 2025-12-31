import React from 'react';
import { render, screen } from '@testing-library/react';
import App from './App';

// Mock the Dashboard component to avoid API calls in tests
jest.mock('./components/Dashboard', () => {
  return function MockDashboard() {
    return <div data-testid="mock-dashboard">Dashboard Component</div>;
  };
});

describe('App Component', () => {
  test('renders without crashing', () => {
    render(<App />);
  });

  test('renders Dashboard component', () => {
    render(<App />);
    const dashboardElement = screen.getByTestId('mock-dashboard');
    expect(dashboardElement).toBeInTheDocument();
  });

  test('has proper App wrapper', () => {
    const { container } = render(<App />);
    const appDiv = container.querySelector('.App');
    expect(appDiv).toBeInTheDocument();
  });
});
