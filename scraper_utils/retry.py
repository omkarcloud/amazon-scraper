"""The retry loop the fetch modules share.

A site keeps its own `_retrying` and passes in what is its own: which error is
retryable and what to do with a failure (usually: drop the session; on_error
gets the exception, or is None when there is nothing to do). The
attempt count and backoff are read from the site's config on every pass, so a
test that changes config.MAX_RETRIES or patches time.sleep still takes effect."""
import time


def run(fn, cfg, retry_on, on_error):
    """fn() with up to cfg.MAX_RETRIES attempts. After a `retry_on` failure:
    on_error(e), then sleep cfg.RETRY_BACKOFF * attempt unless it was the last
    attempt. Any other exception surfaces at once; the last retryable one is
    raised when the attempts run out."""
    last = None
    for attempt in range(1, cfg.MAX_RETRIES + 1):
        try:
            return fn()
        except retry_on as e:
            last = e
            if on_error is not None:
                on_error(e)
            if attempt < cfg.MAX_RETRIES:
                time.sleep(cfg.RETRY_BACKOFF * attempt)
    raise last


def run_flagged(fn, cfg, retry_on, on_error, local, flag="past_gateway"):
    """run(), for sites with a fallback exit ladder: `local.<flag>` is False
    for every attempt but the last, where it is True, so the session built for
    the last attempt may ride the exit behind the gateway. The flag is reset
    when the call ends, whatever the outcome."""
    last = None
    setattr(local, flag, False)
    try:
        for attempt in range(1, cfg.MAX_RETRIES + 1):
            try:
                return fn()
            except retry_on as e:
                last = e
                if on_error is not None:
                    on_error(e)
            if attempt < cfg.MAX_RETRIES:
                setattr(local, flag, attempt + 1 == cfg.MAX_RETRIES)
                time.sleep(cfg.RETRY_BACKOFF * attempt)
        raise last
    finally:
        setattr(local, flag, False)
