import { useEffect, useState, useRef } from 'react'

export default function Home() {
  const [messages, setMessages] = useState([])
  const wsRef = useRef(null)

  useEffect(() => {
    const wsUrl = process.env.NEXT_PUBLIC_WS_URL || 'ws://localhost:8000'
    wsRef.current = new WebSocket(wsUrl)

    wsRef.current.onopen = () => {
      console.log('WebSocket connected to', wsUrl)
    }

    wsRef.current.onmessage = (evt) => {
      try {
        const m = JSON.parse(evt.data)
        // show only fraud predictions where prediction === 1
        if (m.prediction === 1 || (m.is_fraud === 1)) {
          setMessages((prev) => [m, ...prev].slice(0, 200))
        }
      } catch (e) {
        console.error('Invalid message', e)
      }
    }

    wsRef.current.onclose = () => console.log('WebSocket closed')

    return () => {
      if (wsRef.current) wsRef.current.close()
    }
  }, [])

  return (
    <div style={{ padding: 20, fontFamily: 'Arial, sans-serif' }}>
      <h1>Fraud Detection — Realtime Dashboard</h1>
      <p>Showing recent transactions classified as fraud (real-time)</p>

      <div style={{ marginTop: 16 }}>
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          <thead>
            <tr>
              <th style={{ textAlign: 'left', borderBottom: '1px solid #ddd', padding: '8px' }}>Txn ID</th>
              <th style={{ textAlign: 'left', borderBottom: '1px solid #ddd', padding: '8px' }}>User</th>
              <th style={{ textAlign: 'left', borderBottom: '1px solid #ddd', padding: '8px' }}>Score</th>
              <th style={{ textAlign: 'left', borderBottom: '1px solid #ddd', padding: '8px' }}>Model</th>
              <th style={{ textAlign: 'left', borderBottom: '1px solid #ddd', padding: '8px' }}>Timestamp</th>
            </tr>
          </thead>
          <tbody>
            {messages.map((m, i) => (
              <tr key={m.transaction_id + '_' + i}>
                <td style={{ padding: '8px', borderBottom: '1px solid #f1f1f1' }}>{m.transaction_id}</td>
                <td style={{ padding: '8px', borderBottom: '1px solid #f1f1f1' }}>{m.user_id}</td>
                <td style={{ padding: '8px', borderBottom: '1px solid #f1f1f1' }}>{(m.score || m.probability || 0).toFixed ? (m.score || 0).toFixed(4) : m.score}</td>
                <td style={{ padding: '8px', borderBottom: '1px solid #f1f1f1' }}>{m.model || '-'}</td>
                <td style={{ padding: '8px', borderBottom: '1px solid #f1f1f1' }}>{new Date((m.timestamp || Date.now()) * 1000).toLocaleString()}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
