from prometheus_client import Counter
EVENTS_RECEIVED = Counter('collector_events_received_total', 'Total events received')
