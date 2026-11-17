"""Helpers shared by the site packages, so a site keeps only what is its own.

Rules for anything added here:

- Standard library only at import; curl_cffi is imported inside the function
  that needs it. No `config`, `egress`, `identity_pool` or `browser_client`:
  a site passes in what a helper needs, read at call time.
- Nothing secret and no proxy or gateway names. This folder is copied as-is
  into every open-source scraper repo.
- A site module keeps its own names. It imports a helper under the name it
  already used (`from scraper_utils.values import to_int_round as to_int`) or
  keeps a one-line wrapper, and module state such as `_local` stays in the site
  module, so code and tests that reach for `parsers.to_int` or
  `fetch._drop_session` keep working.
"""
