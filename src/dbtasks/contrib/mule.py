"""
uWSGI example mule
==================

A uWSGI mule that starts a `dbtasks.runner.Runner`. Add this to your application using
the `--mule=dbtasks.contrib.mule:taskrunner` command-line option, or by adding the
following to your XML config:

```
<uwsgi>
    ...
    <mule>dbtasks.contrib.mule:taskrunner</mule>
</uwsgi>
```
"""

import signal

from dbtasks.runner import Runner


def taskrunner():
    runner = Runner()

    def _handler(signum, frame):
        runner.stop()

    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT, _handler)
    runner.run()
