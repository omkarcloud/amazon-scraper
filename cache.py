"""The hosted service caches responses; this repo does not. The one thing the
amazon module needs from that layer is the marker it wraps a result in to say
"return this, do not store it"."""


class DontCache:
    """Wrap a result to return it without storing it."""

    def __init__(self, data):
        self.data = data
