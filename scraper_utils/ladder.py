"""The fallback exit ladder some sites walk after a block: own IP, then the
gateway, then a residential exit. The step and its deadline stay in the site
module; this is only the arithmetic."""
import time


def current_step(fallback_step, fallback_until, local, gateway_step):
    """The ladder step a new session should ride: the site's fallback step
    while its cooldown runs, else 0; one further when this call's last retry
    may pass the gateway (`local.past_gateway`) and the step is a gateway one."""
    step = fallback_step if time.time() < fallback_until else 0
    if getattr(local, "past_gateway", False) and gateway_step(step):
        step += 1
    return step
