"""Running several zero-argument callables at once.

Two result shapes exist on purpose. per_call() returns the results and lets
the first failure propagate; guarded_per_call() never raises and returns
("ok", value) / ("error", exception) pairs, for batch endpoints that report
each item's outcome."""
import threading
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor
from concurrent.futures import wait as wait_futures


def per_call(fns, workers):
    """Run zero-arg callables in parallel; results align with `fns`.
    Exceptions propagate from the first failing call."""
    if not fns:
        return []
    if len(fns) == 1:
        return [fns[0]()]
    with ThreadPoolExecutor(max_workers=min(workers, len(fns))) as ex:
        futures = [ex.submit(fn) for fn in fns]
        return [f.result() for f in futures]


def guarded_per_call(fns, workers):
    """Run zero-arg callables in parallel; each result is ("ok", value) or
    ("error", exception), aligned with `fns`."""
    def guard(fn):
        try:
            return ("ok", fn())
        except Exception as e:
            return ("error", e)
    if not fns:
        return []
    with ThreadPoolExecutor(max_workers=max(1, min(workers, len(fns)))) as ex:
        return list(ex.map(guard, fns))


def windowed(pools, prefix, fns, workers):
    """Run `fns` on the pool for this nesting level, at most `workers` at a
    time; results align with `fns`. `pools` is the site's (outer, inner) pair
    of executors whose threads are named `<prefix>-0` / `<prefix>-1`: a call
    from an outer thread uses the inner pool, a call from an inner thread runs
    inline. The first failure (in `fns` order) propagates once the callables
    already started have finished."""
    name = threading.current_thread().name
    if name.startswith(prefix + "-1"):
        return [fn() for fn in fns]
    pool = pools[1] if name.startswith(prefix + "-0") else pools[0]
    results = [None] * len(fns)
    pending, errors, nxt = {}, {}, 0
    while nxt < len(fns) or pending:
        while nxt < len(fns) and len(pending) < workers and not errors:
            pending[pool.submit(fns[nxt])] = nxt
            nxt += 1
        if not pending:
            break
        done, _ = wait_futures(pending, return_when=FIRST_COMPLETED)
        for future in done:
            index = pending.pop(future)
            try:
                results[index] = future.result()
            except BaseException as e:
                errors[index] = e
    if errors:
        raise errors[min(errors)]
    return results


def soft(fn, default, errors):
    """Wrap an optional lookup: a failure in `errors` yields `default`
    instead of failing the endpoint."""
    def run():
        try:
            return fn()
        except errors:
            return default
    return run


def gather(run_parallel, mapping, errors):
    """{key: callable} in parallel -> {key: result}; a failure in `errors`
    for one key yields None instead of failing the set."""
    keys = list(mapping)
    results = run_parallel([soft(mapping[k], None, errors) for k in keys])
    return dict(zip(keys, results))


def gather_optional(run_parallel, mapping, optional, errors):
    """{key: callable} in parallel -> {key: result}. Only the keys named in
    `optional` yield None on a failure in `errors`; any other key fails the set."""
    keys = list(mapping)
    results = run_parallel([soft(mapping[k], None, errors) if k in optional else mapping[k] for k in keys])
    return dict(zip(keys, results))


def per_call_map(fns, workers):
    """per_call(), through Executor.map: after a failure the callables not yet
    started are cancelled instead of run."""
    if not fns:
        return []
    if len(fns) == 1:
        return [fns[0]()]
    with ThreadPoolExecutor(max_workers=min(workers, len(fns))) as pool:
        return list(pool.map(lambda f: f(), fns))


def on_pool(pool, fns):
    """Run zero-arg callables on the site's own long-lived pool; results align
    with `fns`. A single callable runs on the caller's thread."""
    if not fns:
        return []
    if len(fns) == 1:
        return [fns[0]()]
    futures = [pool.submit(fn) for fn in fns]
    return [f.result() for f in futures]


def on_pool_caller_first(pool, fns):
    """on_pool(), with the first callable run on the caller's thread while the
    rest are on the pool (one worker fewer per call)."""
    if not fns:
        return []
    if len(fns) == 1:
        return [fns[0]()]
    futures = [pool.submit(fn) for fn in fns[1:]]
    first = fns[0]()
    return [first] + [f.result() for f in futures]
