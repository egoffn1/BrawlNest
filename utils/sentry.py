"""Sentry integration (optional)."""

def init_sentry(dsn: str = ""):
    if not dsn:
        return
    try:
        import sentry_sdk
        sentry_sdk.init(dsn=dsn, traces_sample_rate=0.1)
    except ImportError:
        pass
