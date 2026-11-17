"""One holder for every expensive, reusable thing a scraper mints: a
browser-box identity (cookies + UA, 10-80 s on the box), an app bearer, a
key harvested from a home page, a warmed curl session.

Why (audit 2026-11-07): twenty-odd modules each kept their own copy of a
cache, and some kept the minted thing per request THREAD (threading.local).
The pod runs 64 request threads, so a request lands on a thread that has
none and mints again: Temu paid a ~25 s mint for every ~1 s page; a block
wave on transfermarkt made every blocked thread mint; waze minted outside
its lock. This module is the one implementation; a site keeps only its
mint function (and its session class) and never a cache of its own.

    pool = identity_pool.Pool("transfermarkt", mint=_mint)          # shared
    tok = pool.get().value
    ...
    pool.get(replace=used, hint=blocked_exit)      # one re-mint, not one per thread

    pool = identity_pool.Pool("temu", mint=_mint, mode="exclusive",
                              kinds=("product", "listing"), budget={...})
    with pool.lend(market, "product") as ident:    # one borrower at a time
        html = ident.session.get(...)

Two lending modes:

  shared     ONE identity per key that any number of threads use at once (a
             token, a harvested key, a cookie jar replayed on each thread's
             own curl session). get() counts one use. get(replace=ident)
             re-mints only if `ident` is still the current one: after a
             refusal the first thread mints, the others get its result.
  exclusive  SEVERAL identities per key, each lent to one caller at a time
             (an identity that owns a curl session, a warmed session).
             borrow() / give_back(), or `with pool.lend(key, kind)`. Lent
             oldest first, so an identity spends its budget before it ages.

Common to both: single-flight mints per key; a caller that finds nothing
waits (mint_wait) while identities are on their way - a mint in flight, or
(exclusive) a lent one with uses left - and mints itself only when nothing
is coming or the wait ran out (a failed mint wakes the waiters, who then
mint themselves); per-kind use budgets; max_age and the mint's own
expires_at; retire on refusal with compare-and-swap (retiring an identity
another thread already replaced is a no-op, so one refusal wave costs one
mint); optional mint-ahead (one background mint when a key's remaining
uses of a kind fall to mint_ahead_at); an optional process-wide
mints-per-minute budget (box mints need it: browser_client.mint() charges
nothing, only browser_client.identity() does); an optional min_interval
that throttles value-rejection refreshes (a harvested key: a block wave is
not a key rotation, so the second refresh inside the interval returns the
same value and the caller fails as it would have); seeds usable before any
mint.

Nothing mints at startup or while idle: every mint, mint-ahead included,
follows a request (tests/test_no_startup_browsers.py).

The pool never catches the mint function's exceptions: a site keeps its own
mapping of BoxUnavailable / BoxRefused / HTTP errors. MintBudgetExceeded is
the pool's own, raised before the mint function runs.
"""
import threading
import time
from collections import deque
from contextlib import contextmanager

UNLIMITED = 10 ** 9
RETIRE_ROTATES_EXIT = ("refused", "replaced")    # reasons that burn the identity's exit


class MintBudgetExceeded(Exception):
    """mints_per_minute spent: the site maps it (Temu: TemuBoxDown)."""


def _val(x):
    return x() if callable(x) else x


class Identity:
    """What the pool lends. `value` is the site's payload (a token string, a
    dict, a session holder, or None when a subclass carries the fields
    itself). `exit` is the proxy URL it was minted on (the hint, or the
    pool's exit_pool slot), `expires_at` a wall-clock deadline from the
    mint (None = only the pool's max_age / budget retire it)."""

    def __init__(self, value=None, *, exit=None, expires_at=None):
        self.value = value
        self.exit = exit
        self.expires_at = expires_at
        self.key = ""
        self.minted_at = time.monotonic()
        self.uses = {}
        self.dead = False          # refused: never lent again
        self.retired = False       # the pool is done with it (on_retire ran)
        self.seeded = False        # a seed value, not a mint (ignores max_age)
        self.exit_rotated = False  # its exit was already rotated (a replace in flight)

    def __repr__(self):
        return f"<Identity key={self.key!r} uses={self.uses} dead={self.dead} exit={self.exit}>"


class Pool:
    def __init__(self, name, mint, *, mode="shared", kinds=("use",), budget=None, max_age=None,
                 mint_ahead_at=None, mint_wait=60.0, mints_per_minute=None, min_interval=None,
                 seed=None, exit_pool=None, on_retire=None):
        """mint(key, hint) -> Identity | value. Every knob but `mode`,
        `kinds`, `seed` and `exit_pool` may be a callable read on use (pass
        `lambda: config.SITE_X` so a test or a live knob change applies).

        budget      uses per identity: int (every kind) or {kind: int}; None = unlimited
        max_age     seconds since the mint; None = only expires_at / budget
        mint_ahead_at   background mint when a key has at most this many
                    uses of the requested kind left; None = off
        mint_wait   seconds a caller waits for an identity on its way
        mints_per_minute   process-wide cap; MintBudgetExceeded above it
        min_interval    seconds between two value-rejection refreshes of one key
        seed        {key: value} usable before any mint
        exit_pool   browser_client.ExitPool the mints run on (hint =
                    exit_pool.get(key)); rotated when an identity is refused
        on_retire   hook(ident, reason) when the pool is done with an identity
                    (reason: refused | replaced | spent | expired | clear);
                    close a session, count a refusal here"""
        if mode not in ("shared", "exclusive"):
            raise ValueError(f"{name}: mode must be shared or exclusive, not {mode!r}")
        self.name = name
        self.mode = mode
        self.kinds = tuple(kinds) or ("use",)
        self._mint_fn = mint
        self._budget_knob = budget
        self.max_age = max_age
        self.mint_ahead_at = mint_ahead_at
        self.mint_wait = mint_wait
        self.mints_per_minute = mints_per_minute
        self.min_interval = min_interval
        self._seed = dict(seed or {})
        self.exit_pool = exit_pool
        self.on_retire = on_retire
        self._cond = threading.Condition()
        self._current = {}      # shared: key -> Identity
        self._spare = {}        # shared: key -> Identity minted ahead
        self._idle = {}         # exclusive: key -> [Identity]
        self._lent = {}         # exclusive: key -> {Identity}
        self._minting = {}      # key -> mints in flight (callers + mint-ahead)
        self._waiting = {}      # (key, kind) -> callers waiting for one on its way
        self._last_mint = {}    # key -> monotonic time of the last mint
        self._mint_times = deque()

    # ---- budgets ------------------------------------------------------------------------

    def _budget(self):
        b = _val(self._budget_knob)
        if b is None:
            return {}
        if isinstance(b, int):
            return {k: b for k in self.kinds}
        return {k: _val(v) for k, v in b.items()}

    def left(self, ident, kind=None):
        """Uses of `kind` this identity may still serve (0 once refused,
        aged out or expired)."""
        if ident.dead:
            return 0
        max_age = _val(self.max_age)
        if max_age and not ident.seeded and time.monotonic() - ident.minted_at > max_age:
            return 0
        if ident.expires_at is not None and time.time() >= ident.expires_at:
            return 0
        limit = self._budget().get(kind or self.kinds[0])
        if limit is None:
            return UNLIMITED
        return max(0, limit - ident.uses.get(kind or self.kinds[0], 0))

    def exhausted(self, ident):
        return all(self.left(ident, k) <= 0 for k in self.kinds)

    def _why_unusable(self, ident, kind):
        if ident.dead:
            return "refused"
        if ident.expires_at is not None and time.time() >= ident.expires_at:
            return "expired"
        max_age = _val(self.max_age)
        if max_age and not ident.seeded and time.monotonic() - ident.minted_at > max_age:
            return "expired"
        return "spent"

    def _charge_mint_budget(self):
        cap = _val(self.mints_per_minute)
        if not cap:
            return
        now = time.monotonic()
        while self._mint_times and self._mint_times[0] < now - 60:
            self._mint_times.popleft()
        if len(self._mint_times) >= cap:
            raise MintBudgetExceeded(f"{self.name}: mint budget spent ({cap}/min); retry shortly")
        self._mint_times.append(now)

    # ---- minting (outside the lock) -----------------------------------------------------

    def _mint(self, key, hint):
        with self._cond:
            self._charge_mint_budget()
        if hint is None and self.exit_pool is not None:
            hint = self.exit_pool.get(key if isinstance(key, str) else "")
        try:
            out = self._mint_fn(key, hint)
        finally:
            # a failed mint counts too: min_interval throttles a harvest that
            # keeps failing as much as one that keeps returning the same value
            with self._cond:
                self._last_mint[key] = time.monotonic()
        ident = out if isinstance(out, Identity) else Identity(out)
        ident.key = key
        if ident.exit is None:
            ident.exit = hint
        return ident

    def _rotate_exit_locked(self, ident):
        if self.exit_pool is None or not ident.exit or ident.exit_rotated:
            return
        ident.exit_rotated = True
        try:
            self.exit_pool.rotate(ident.exit, ident.key if isinstance(ident.key, str) else "")
        except Exception as e:
            print(f"{self.name}: exit rotate failed: {type(e).__name__}: {e}")

    def _mint_counted(self, key, hint, after):
        """Mint with the in-flight count held; `after(ident)` runs under the
        lock once it landed; waiters are woken either way."""
        try:
            ident = self._mint(key, hint)
        except BaseException:
            with self._cond:
                self._minting[key] -= 1
                self._cond.notify_all()
            raise
        with self._cond:
            # one step: a waiter must never see the mint finished but the
            # identity not yet installed (it would count nothing coming and mint)
            self._minting[key] -= 1
            after(ident)
            self._cond.notify_all()
        return ident

    def _mint_ahead(self, key, kind):
        try:
            self._mint_counted(key, None, lambda ident: self._add_spare_locked(key, ident))
        except Exception as e:
            print(f"{self.name}: mint-ahead for {key!r} failed: {type(e).__name__}: {e}")

    def _start_mint_ahead_locked(self, key, kind):
        """One background mint when the key's live identities have at most
        mint_ahead_at uses of `kind` left and nothing is in flight."""
        at = _val(self.mint_ahead_at)
        if at is None or self._minting.get(key, 0):
            return
        if self.mode == "shared":
            live = [i for i in (self._current.get(key), self._spare.get(key)) if i is not None]
        else:
            live = self._idle.get(key, []) + list(self._lent.get(key, ()))
        if sum(self.left(i, kind) for i in live) > at:
            return
        self._minting[key] = self._minting.get(key, 0) + 1
        threading.Thread(target=self._mint_ahead, args=(key, kind), name=f"{self.name}-mint-ahead",
                         daemon=True).start()

    def _add_spare_locked(self, key, ident):
        if self.mode == "exclusive":
            self._idle.setdefault(key, []).append(ident)
        elif self._spare.get(key) is None:
            self._spare[key] = ident
        else:
            self._idle.setdefault(key, []).append(ident)     # two spares: keep the extra as idle

    # ---- retiring ----------------------------------------------------------------------

    def _retire_locked(self, ident, reason):
        ident.dead = True
        if ident.retired:
            return
        ident.retired = True
        if self.on_retire is not None:
            try:
                self.on_retire(ident, reason)
            except Exception as e:
                print(f"{self.name}: on_retire failed: {type(e).__name__}: {e}")
        if reason in RETIRE_ROTATES_EXIT:
            self._rotate_exit_locked(ident)

    def retire(self, ident, reason="refused"):
        """The caller found `ident` refused: never lend it again. True when
        this call retired it, False when it was already gone (another thread
        retired or replaced it first: one refusal wave, one retirement)."""
        with self._cond:
            key = ident.key
            if self.mode == "shared":
                for store in (self._current, self._spare):
                    if store.get(key) is ident:
                        del store[key]
                        self._retire_locked(ident, reason)
                        self._cond.notify_all()
                        return True
                idle = self._idle.get(key, [])
                if ident in idle:
                    idle.remove(ident)
                    self._retire_locked(ident, reason)
                    return True
                return False
            idle = self._idle.get(key, [])
            if ident in idle:
                idle.remove(ident)
                self._retire_locked(ident, reason)
                self._cond.notify_all()
                return True
            if ident in self._lent.get(key, ()) and not ident.dead:
                ident.dead = True          # retired when it comes back
                return True
            return False

    def retire_where(self, pred, reason="refused"):
        """retire() every identity for which pred(ident) is true."""
        with self._cond:
            found = [i for i in self._current.values() if pred(i)]
            found += [i for i in self._spare.values() if pred(i)]
            found += [i for idents in self._idle.values() for i in idents if pred(i)]
            found += [i for idents in self._lent.values() for i in idents if pred(i)]
        return sum(1 for i in found if self.retire(i, reason))

    def reset(self):
        """Tests: clear() plus the mint-rate and refresh-interval history."""
        self.clear()
        with self._cond:
            self._mint_times.clear()
            self._last_mint.clear()

    def clear(self):
        """Retire everything (tests, a config reload)."""
        with self._cond:
            idents = list(self._current.values()) + list(self._spare.values())
            idents += [i for idents in self._idle.values() for i in idents]
            self._current.clear()
            self._spare.clear()
            self._idle.clear()
            for ident in idents:
                self._retire_locked(ident, "clear")
            for ident in [i for idents in self._lent.values() for i in idents]:
                ident.dead = True
            self._cond.notify_all()

    # ---- shared mode -------------------------------------------------------------------

    def _usable_locked(self, key, kind):
        """The current identity of `key` if it can serve `kind`; promotes a
        spare / the seed; retires what cannot serve any kind."""
        ident = self._current.get(key)
        if ident is not None and self.left(ident, kind) > 0:
            return ident
        if ident is not None and self.exhausted(ident):
            del self._current[key]
            self._retire_locked(ident, self._why_unusable(ident, kind))
            ident = None
        if ident is None:
            spare = self._spare.pop(key, None)
            if spare is None and self._idle.get(key):
                spare = self._idle[key].pop(0)
            if spare is not None:
                self._current[key] = spare
                return spare if self.left(spare, kind) > 0 else None
            if key in self._seed:
                ident = Identity(self._seed[key])
                ident.key = key
                ident.seeded = True
                ident.minted_at = float("-inf")
                self._current[key] = ident
                return ident if self.left(ident, kind) > 0 else None
        return None

    def peek(self, key="", kind=None):
        """The current shared identity, or None; no use charged, no mint."""
        with self._cond:
            return self._usable_locked(key, kind or self.kinds[0])

    def get(self, key="", kind=None, *, replace=None, hint=None, mint=True):
        """The shared identity of `key`, charging one use of `kind`.

        replace=ident   `ident` was refused: re-mint only if it is still the
                        current one (else the caller gets what replaced it);
                        inside min_interval of the last mint (failed mints
                        count) the same identity comes back unchanged (the
                        caller judges the value). The refused identity stays
                        current until the new one lands, so a failed mint
                        leaves the last known value, not nothing; its exit is
                        rotated at once so the mint runs on a fresh one.
        hint            the exit the mint must run on (a challenged session's)
        mint=False      None instead of minting"""
        kind = kind or self.kinds[0]
        deadline = time.monotonic() + _val(self.mint_wait)
        replaced = None
        with self._cond:
            while True:
                ident = self._usable_locked(key, kind)
                if ident is not None and ident is not replace:
                    ident.uses[kind] = ident.uses.get(kind, 0) + 1
                    self._start_mint_ahead_locked(key, kind)
                    return ident
                if not mint:
                    return None
                if ident is not None:          # ident is replace: refused by the caller
                    # a spare already minted (mint-ahead, a concurrent mint that
                    # landed second) replaces it for free: no mint for this wave
                    spare = self._spare.pop(key, None)
                    if spare is None and self._idle.get(key):
                        spare = self._idle[key].pop(0)
                    if spare is not None:
                        if self.left(spare, kind) > 0:
                            del self._current[key]
                            self._retire_locked(ident, "replaced")
                            self._current[key] = spare
                            spare.uses[kind] = spare.uses.get(kind, 0) + 1
                            self._start_mint_ahead_locked(key, kind)
                            self._cond.notify_all()
                            return spare
                        self._retire_locked(spare, self._why_unusable(spare, kind))
                    interval = _val(self.min_interval)
                    if (interval and not self._minting.get(key)
                            and time.monotonic() - self._last_mint.get(key, float("-inf")) < interval):
                        ident.uses[kind] = ident.uses.get(kind, 0) + 1
                        return ident           # throttled: same value, the caller judges
                    replaced = ident
                    self._rotate_exit_locked(ident)
                # one shared mint serves every waiter: wait while one is in flight
                now = time.monotonic()
                if not self._minting.get(key) or now >= deadline:
                    break
                self._cond.wait(deadline - now)
            self._minting[key] = self._minting.get(key, 0) + 1

        def install(ident):
            cur = self._current.get(key)
            if replaced is not None and cur is replaced:
                del self._current[key]
                self._retire_locked(replaced, "replaced")
                cur = None
            # a concurrent mint (a waiter that timed out) may have landed
            # first: the newer one serves, the other is kept as a spare
            if cur is not None and self.left(cur, kind) > 0:
                self._add_spare_locked(key, ident)
                ident = cur
            else:
                if cur is not None:
                    # displaced, never dropped silently: on_retire must run
                    # (a session is closed there), or it keeps its other kinds
                    if self.exhausted(cur):
                        self._retire_locked(cur, self._why_unusable(cur, kind))
                    else:
                        self._add_spare_locked(key, cur)
                self._current[key] = ident
            ident.uses[kind] = ident.uses.get(kind, 0) + 1
            got[0] = ident

        got = [None]
        self._mint_counted(key, hint, install)
        return got[0]

    # ---- exclusive mode ----------------------------------------------------------------

    def _take_idle_locked(self, key, kind):
        keep = []
        for ident in self._idle.get(key, []):
            if self.exhausted(ident):
                self._retire_locked(ident, self._why_unusable(ident, kind))
            else:
                keep.append(ident)
        self._idle[key] = keep
        usable = [i for i in keep if self.left(i, kind) > 0]
        if not usable:
            return None
        ident = min(usable, key=lambda i: i.minted_at)
        keep.remove(ident)
        return ident

    def _lend_locked(self, key, ident, kind):
        ident.uses[kind] = ident.uses.get(kind, 0) + 1
        self._lent.setdefault(key, set()).add(ident)
        self._start_mint_ahead_locked(key, kind)

    def borrow(self, key="", kind=None):
        """Lend one identity of `key` for ONE use of `kind`; give_back() it."""
        if self.mode != "exclusive":
            raise RuntimeError(f"{self.name}: borrow() is for exclusive pools; use get()")
        kind = kind or self.kinds[0]
        deadline = time.monotonic() + _val(self.mint_wait)
        with self._cond:
            while True:
                ident = self._take_idle_locked(key, kind)
                if ident is not None:
                    self._lend_locked(key, ident, kind)
                    return ident
                now = time.monotonic()
                coming = self._minting.get(key, 0) + sum(
                    1 for i in self._lent.get(key, ()) if self.left(i, kind) > 0)
                wk = (key, kind)
                if coming <= self._waiting.get(wk, 0) or now >= deadline:
                    break
                self._waiting[wk] = self._waiting.get(wk, 0) + 1
                try:
                    self._cond.wait(deadline - now)
                finally:
                    self._waiting[wk] -= 1
            self._minting[key] = self._minting.get(key, 0) + 1
        return self._mint_counted(key, None, lambda ident: self._lend_locked(key, ident, kind))

    def give_back(self, ident):
        """Return a borrowed identity: back to the pool, or retired when it
        was refused (ident.dead), spent or aged out."""
        with self._cond:
            key = ident.key
            lent = self._lent.get(key, set())
            if ident not in lent:
                return                     # not lent (given back twice): never duplicate it
            lent.discard(ident)
            if self.exhausted(ident):
                self._retire_locked(ident, self._why_unusable(ident, self.kinds[0]))
            else:
                self._idle.setdefault(key, []).append(ident)
            self._cond.notify_all()

    @contextmanager
    def lend(self, key="", kind=None):
        """`with pool.lend(key, kind) as ident:` - borrow / give_back; set
        ident.dead = True inside the block to retire it on the way out."""
        ident = self.borrow(key, kind)
        try:
            yield ident
        finally:
            self.give_back(ident)

    # ---- introspection -----------------------------------------------------------------

    def stats(self, key=""):
        with self._cond:
            if self.mode == "shared":
                live = [i for i in (self._current.get(key), self._spare.get(key)) if i is not None]
                live += self._idle.get(key, [])
                out = {"current": int(self._current.get(key) is not None),
                       "spare": int(self._spare.get(key) is not None) + len(self._idle.get(key, []))}
            else:
                live = self._idle.get(key, []) + list(self._lent.get(key, ()))
                out = {"idle": len(self._idle.get(key, [])), "lent": len(self._lent.get(key, ()))}
            out["minting"] = self._minting.get(key, 0)
            out["left"] = {k: sum(self.left(i, k) for i in live) for k in self.kinds}
            return out
