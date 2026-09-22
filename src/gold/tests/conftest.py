import os
import sys

# Scripts in src/gold/ import `utils.*` relative to their own directory
# (that's how spark-submit resolves `local:///opt/spark/work-dir/src/gold/
# <script>.py`'s own imports -- see the Dockerfile/gold_dag.py). Mirror
# that here so these tests import the real `src/gold/utils` package.
#
# IMPORTANT: run this suite in its own pytest invocation (e.g.
# `pytest src/gold/tests`), never combined with src/silver/tests in the
# same run. gold and silver each have their own `utils` package, and
# Python's sys.modules caches whichever one is imported first
# process-wide -- the second suite would then silently resolve to the
# first suite's `utils` instead of its own, producing confusing import
# errors or (worse) silently wrong results. This isn't a workaround:
# gold and silver scripts are never co-resident in the same Python
# process in production either (each runs as its own standalone
# spark-submit job), so testing them in separate invocations mirrors
# that reality rather than working around it.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
