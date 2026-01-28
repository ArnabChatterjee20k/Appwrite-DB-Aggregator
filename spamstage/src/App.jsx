import { useEffect, useMemo, useState } from 'react'
import { Client, Realtime } from 'appwrite'
import './App.css'

function RealtimeListener({ id, endpoint, projectId, channels, running, runToken }) {
  const [status, setStatus] = useState('idle') // idle | connecting | listening | error
  const [lastEvent, setLastEvent] = useState(null)
  const [eventCount, setEventCount] = useState(0)
  const [error, setError] = useState('')

  useEffect(() => {
    let unsubscribe = null
    let realtime = null

    if (!running) {
      setStatus('idle')
      setLastEvent(null)
      setEventCount(0)
      setError('')
      return () => {}
    }

    async function connect() {
      try {
        setStatus('connecting')
        setError('')

        const client = new Client().setEndpoint(endpoint).setProject(projectId)
        realtime = new Realtime(client)

        const list = channels.filter(Boolean)
        if (!list.length) {
          throw new Error('No channels provided')
        }

        unsubscribe = realtime.subscribe(list, (event) => {
          setEventCount((c) => c + 1)
          setLastEvent({
            ts: new Date().toLocaleTimeString(),
            event,
          })
        })

        // Appwrite doesn't guarantee an "open" callback; mark as listening once subscribed.
        setStatus('listening')
      } catch (err) {
        console.error(err)
        setStatus('error')
        setError(err.message || 'Failed to connect')
      }
    }

    connect()

    return () => {
      if (unsubscribe) {
        try {
          unsubscribe()
        } catch (err) {
          console.error('unsubscribe failed', err)
        }
      }
      if (realtime) {
        try {
          realtime.close()
        } catch (err) {
          console.error('close failed', err)
        }
      }
    }
    // rerun when runToken changes to recreate connections
  }, [endpoint, projectId, channels, running, runToken])

  return (
    <div className="listener">
      <div className="listener__header">
        <span className="listener__title">Listener #{id}</span>
        <span className={`badge badge--${status}`}>
          {status === 'idle' && 'idle'}
          {status === 'connecting' && 'connecting'}
          {status === 'listening' && `listening (${eventCount})`}
          {status === 'error' && 'error'}
        </span>
      </div>
      <div className="listener__body">
        <div className="listener__channels">
          {channels.map((c) => (
            <span key={c} className="chip">
              {c}
            </span>
          ))}
        </div>
        {error && <div className="error">Error: {error}</div>}
        {lastEvent ? (
          <pre className="event">
{JSON.stringify(lastEvent.event, null, 2)}
          </pre>
        ) : (
          <div className="empty">No events yet</div>
        )}
        {lastEvent && <div className="timestamp">Last at {lastEvent.ts}</div>}
      </div>
    </div>
  )
}

function App() {
  const [endpoint, setEndpoint] = useState('https://fra.stage.cloud.appwrite.io/v1')
  const [projectId, setProjectId] = useState('69366a0c001b90061760')
  const [databaseId, setDatabaseId] = useState('auto-generated-db')
  const [collections, setCollections] = useState('users, products, posts, events')
  const [count, setCount] = useState(10)
  const [running, setRunning] = useState(false)
  const [runToken, setRunToken] = useState(0)

  const channels = useMemo(() => {
    return collections
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean)
      .map((c) => `databases.${databaseId}.collections.${c}.documents`)
  }, [collections, databaseId])

  const listeners = Array.from({ length: Math.max(1, Number(count) || 1) }, (_, i) => i + 1)

  const handleStart = () => {
    setRunToken((t) => t + 1)
    setRunning(true)
  }

  const handleStop = () => {
    setRunning(false)
  }

  return (
    <div className="page">
      <header className="header">
        <div>
          <h1>Realtime Stage</h1>
          <p className="sub">Spawns multiple Appwrite Realtime listeners.</p>
        </div>
        <div className="actions">
          <button className="btn" onClick={handleStart} disabled={running}>
            Start
          </button>
          <button className="btn btn--secondary" onClick={handleStop} disabled={!running}>
            Stop
          </button>
        </div>
      </header>

      <section className="panel">
        <div className="field">
          <label>Endpoint</label>
          <input value={endpoint} onChange={(e) => setEndpoint(e.target.value)} />
        </div>
        <div className="field">
          <label>Project ID</label>
          <input value={projectId} onChange={(e) => setProjectId(e.target.value)} />
        </div>
        <div className="field">
          <label>Database ID</label>
          <input value={databaseId} onChange={(e) => setDatabaseId(e.target.value)} />
        </div>
        <div className="field">
          <label>Collections (comma)</label>
          <input value={collections} onChange={(e) => setCollections(e.target.value)} />
        </div>
        <div className="field">
          <label>Number of listeners</label>
          <input
            type="number"
            min="1"
            max="50"
            value={count}
            onChange={(e) => setCount(e.target.value)}
          />
        </div>
      </section>

      <section className="grid">
        {listeners.map((id) => (
          <RealtimeListener
            key={`${id}-${runToken}`}
            id={id}
            endpoint={endpoint}
            projectId={projectId}
            channels={channels}
            running={running}
            runToken={runToken}
          />
        ))}
      </section>
    </div>
  )
}

export default App
