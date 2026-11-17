"""Small in-process caches for lookups a site repeats (reference lists, ids).

The dict and its lock stay in the site module and are passed in, so the site's
tests can clear or inspect them as before."""
import json
import time


def memoized(store, lock, cap, key, fn, ttl):
    """fn() cached under `key` for `ttl` seconds. `store` is an OrderedDict
    kept to the `cap` most recently used entries. fn() runs outside the lock,
    so two threads may both compute a missing key."""
    now = time.monotonic()
    with lock:
        hit = store.get(key)
        if hit and now - hit[0] < ttl:
            store.move_to_end(key)
            return hit[1]
    value = fn()
    with lock:
        store[key] = (now, value)
        store.move_to_end(key)
        while len(store) > cap:
            store.popitem(last=False)
    return value


def expiring(store, lock, key, ttl, build):
    """build() cached under `key` until `ttl` seconds from now; never evicted."""
    now = time.time()
    with lock:
        hit = store.get(key)
        if hit and hit[0] > now:
            return hit[1]
    value = build()
    with lock:
        store[key] = (now + ttl, value)
    return value


def remember(store, lock, key, ttl, value):
    """Put a value an expiring() lookup would return, without building it."""
    with lock:
        store[key] = (time.time() + ttl, value)


def shared_raw(memoized, prefix, memo_ttl, name, params, build, ttl):
    """An upstream payload shared by several routes: the site's in-process
    memo, then the service's response cache (pods share it) when one is
    installed, then build(). Without a `cache` module (an open-source repo)
    it is the in-process memo alone."""
    try:
        from cache import cached_call
    except ImportError:
        cached_call = None
    seconds = ttl.total_seconds() if hasattr(ttl, "total_seconds") else ttl
    key = (name, json.dumps(params, sort_keys=True))
    if cached_call is None:
        return memoized(key, build, ttl=min(seconds, memo_ttl))
    return memoized(key, lambda: cached_call(f"{prefix}{name}", params, lambda **_: build(), ttl),
                    ttl=min(seconds, memo_ttl))
