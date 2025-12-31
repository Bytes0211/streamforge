# StreamForge Frontend Testing

## Test Summary

✅ **All 50 tests passing**  
📊 **67.85% code coverage**  
⚡ **Test execution time: ~1.5 seconds**

## Test Suites

### 1. App.test.js (3 tests)
Tests the root App component including:
- Renders without crashing
- Renders Dashboard component
- Has proper App wrapper div

### 2. Dashboard.test.js (6 tests)
Tests the main Dashboard container component:
- Renders loading state initially
- Renders dashboard after data loads
- Renders Recent Events section
- Renders Event Metrics section
- Displays error message when API fails
- Calls API with correct parameters

### 3. EventList.test.js (8 tests)
Tests the event list table component:
- Renders without crashing
- Displays "No events" message when empty
- Renders table headers correctly
- Renders event data correctly
- Formats values with 2 decimal places
- Renders correct number of rows
- Handles null/undefined props gracefully

### 4. StatsCards.test.js (9 tests)
Tests the statistics cards component:
- Renders without crashing
- Displays correct total events count
- Displays correct unique users count
- Calculates and displays average value
- Displays correct metrics windows count
- Renders all four stat card titles
- Handles empty/null/undefined props gracefully
- Renders four stat cards

### 5. MetricsChart.test.js (10 tests)
Tests the Recharts visualization component:
- Renders without crashing
- Displays "No metrics data" when empty
- Renders summary statistics
- Calculates total count correctly
- Calculates average value correctly
- Displays correct number of windows
- Renders Recharts components (chart, axes, grid)
- Handles incomplete data gracefully

### 6. api.test.js (14 tests)
Tests the API service and mock data generation:
- Returns array of events in mock mode
- Events have required fields
- Respects limit parameter
- Generates different event types
- Returns array of metrics
- Metrics have required fields
- Metric values are valid numbers
- Min/max values are consistent with avg
- Generates realistic timestamps
- Generates different user IDs
- Generates valid numeric values

## Code Coverage

```
File                 | % Stmts | % Branch | % Funcs | % Lines
---------------------|---------|----------|---------|----------
All files            |   65.56 |    43.68 |   55.81 |   67.85
src/components       |   93.65 |    82.92 |   88.23 |   93.33
  Dashboard.jsx      |     100 |      100 |     100 |     100
  EventList.jsx      |     100 |    83.33 |     100 |     100
  MetricsChart.jsx   |   76.47 |    76.19 |   71.42 |   73.33
  StatsCards.jsx     |     100 |       90 |     100 |     100
src/services         |   53.03 |    33.33 |   33.33 |   58.62
  api.js             |   53.03 |    33.33 |   33.33 |   58.62
```

### Coverage Notes
- **High coverage** on all React components (93%+)
- **Dashboard.jsx** has 100% coverage
- **api.js** has lower coverage because only mock data mode is tested
- Real AWS DynamoDB calls are not tested (would require AWS setup)

## Running Tests

### Run all tests
```bash
npm test
```

### Run tests in watch mode (default)
```bash
npm test
```

### Run tests once with coverage
```bash
npm test -- --watchAll=false --coverage
```

### Run specific test file
```bash
npm test Dashboard.test.js
```

### Run tests matching pattern
```bash
npm test -- --testNamePattern="renders"
```

## Test Configuration

### Setup Files
- `src/setupTests.js` - Jest configuration
  - Imports @testing-library/jest-dom
  - Mocks ResizeObserver for Recharts

### Mocking Strategy
1. **API Service**: Mocked in Dashboard tests to avoid real API calls
2. **Child Components**: Mocked in Dashboard tests to isolate testing
3. **Recharts**: Mocked in MetricsChart tests to avoid rendering issues
4. **AWS SDK**: Mocked in api tests to avoid requiring AWS credentials

## Testing Best Practices

### What We Test
✅ Component rendering  
✅ User interactions  
✅ Data display and formatting  
✅ Error handling  
✅ Edge cases (null/undefined/empty data)  
✅ API integration (mocked)  
✅ Mock data generation  

### What We Don't Test
❌ Real AWS DynamoDB calls (requires infrastructure)  
❌ Amplify authentication (requires AWS setup)  
❌ Visual regression (no visual testing tools)  
❌ E2E user flows (would require Cypress/Playwright)  

## Continuous Integration

These tests can be integrated into CI/CD pipelines:

```yaml
# Example GitHub Actions workflow
- name: Run tests
  run: npm test -- --watchAll=false --coverage
  
- name: Upload coverage
  uses: codecov/codecov-action@v3
```

## Test Data

### Mock Events
- Generated with realistic IDs, types, user IDs, values
- Event types: click, view, purchase, signup, logout
- Values: Random between 0-100
- Timestamps: Current time minus intervals

### Mock Metrics
- Generated with windowed aggregations
- Includes count, sum, avg, min, max
- Time windows: 1 hour intervals
- Realistic statistical relationships

## Future Improvements

1. **E2E Testing**: Add Cypress or Playwright for full user flows
2. **Visual Testing**: Add Storybook with visual regression tests
3. **Integration Tests**: Test real AWS DynamoDB integration (in staging)
4. **Performance Testing**: Add React performance profiling tests
5. **Accessibility Testing**: Add a11y tests with jest-axe
6. **Snapshot Testing**: Add component snapshot tests

## Troubleshooting

### Tests fail with "ResizeObserver not defined"
- Already fixed in `setupTests.js` with global mock

### Tests fail with Recharts errors
- Mock Recharts components in tests (see MetricsChart.test.js)

### Tests timeout
- Increase Jest timeout: `jest.setTimeout(10000)`
- Check for unresolved promises or infinite loops

### Coverage too low
- Add more test cases for uncovered branches
- Test error paths and edge cases
- Test all component props combinations

## Resources

- [React Testing Library](https://testing-library.com/react)
- [Jest Documentation](https://jestjs.io/)
- [Testing Best Practices](https://kentcdodds.com/blog/common-mistakes-with-react-testing-library)
