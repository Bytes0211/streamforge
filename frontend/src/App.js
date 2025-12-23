import React, { useState, useEffect } from 'react';
import './App.css';
import { get } from 'aws-amplify/api';

function App() {
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const fetchData = async () => {
    setLoading(true);
    setError(null);
    try {
      const restOperation = get({
        apiName: 'streamforge',
        path: '/items'
      });
      const response = await restOperation.response;
      const body = await response.body.json();
      setData(body);
    } catch (err) {
      console.error('Error fetching data:', err);
      setError(err.message || 'Failed to fetch data');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, []);

  return (
    <div className="App">
      <header className="App-header">
        <h1>StreamForge</h1>
        <p>Real-time Data Streaming Platform</p>
      </header>
      
      <main className="App-main">
        <div className="data-section">
          <h2>Processed Stream Data</h2>
          <button onClick={fetchData} disabled={loading}>
            {loading ? 'Loading...' : 'Refresh Data'}
          </button>
          
          {error && (
            <div className="error">
              <p>Error: {error}</p>
            </div>
          )}
          
          {data.length > 0 ? (
            <div className="data-grid">
              {data.map((item, index) => (
                <div key={index} className="data-card">
                  <pre>{JSON.stringify(item, null, 2)}</pre>
                </div>
              ))}
            </div>
          ) : (
            !loading && !error && <p>No data available</p>
          )}
        </div>
      </main>
    </div>
  );
}

export default App;
