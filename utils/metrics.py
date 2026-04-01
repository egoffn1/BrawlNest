"""Prometheus metrics (optional — graceful degradation if not installed)."""
REQUEST_COUNT = REQUEST_LATENCY = BRAWL_API_CALLS = BRAWL_API_ERRORS = None
P2P_MESSAGES = P2P_PEERS = CACHE_HITS = CACHE_MISSES = None
_enabled = False
_initialized = False

def init_metrics():
    global _enabled, REQUEST_COUNT, REQUEST_LATENCY, BRAWL_API_CALLS
    global BRAWL_API_ERRORS, P2P_MESSAGES, P2P_PEERS, CACHE_HITS, CACHE_MISSES, _initialized
    if _initialized:
        return
    try:
        from prometheus_client import Counter, Histogram, Gauge
        REQUEST_COUNT    = Counter("http_requests_total", "HTTP requests", ["method", "path", "status"])
        REQUEST_LATENCY  = Histogram("http_request_duration_seconds", "HTTP latency", ["path"])
        BRAWL_API_CALLS  = Counter("brawl_api_calls_total", "Brawl API calls", ["endpoint"])
        BRAWL_API_ERRORS = Counter("brawl_api_errors_total", "Brawl API errors", ["status"])
        P2P_MESSAGES     = Counter("p2p_messages_total", "P2P messages", ["msg_type"])
        P2P_PEERS        = Gauge("p2p_peers_count", "Active P2P peers")
        CACHE_HITS       = Counter("cache_hits_total", "Cache hits", ["cache"])
        CACHE_MISSES     = Counter("cache_misses_total", "Cache misses", ["cache"])
        _enabled = True
        _initialized = True
    except ImportError:
        pass

def inc(counter, *labels):
    if counter is not None:
        try:
            counter.labels(*labels).inc()
        except Exception:
            pass

def observe(histogram, *labels, value: float):
    if histogram is not None:
        try:
            histogram.labels(*labels).observe(value)
        except Exception:
            pass
