"""Debug dumps: keep the raw body of a failed response for a post-mortem."""
import os


def dump(env_var, name, text, encoding=None, ext=".txt", render=None):
    """Write `text` to $<env_var>/<name><ext>. Does nothing when the variable
    is unset or the text is empty; a write failure is swallowed. `render`
    turns a non-string (a decoded JSON body) into text."""
    dbg = os.environ.get(env_var, "")
    if dbg and text:
        try:
            os.makedirs(dbg, exist_ok=True)
            with open(os.path.join(dbg, name + ext), "w", encoding=encoding) as f:
                f.write(text if render is None or isinstance(text, str) else render(text))
        except OSError:
            pass
