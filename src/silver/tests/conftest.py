import os
import sys

# Scripts in src/silver/ import `utils.*` relative to their own directory,
# same as src/gold/ (see src/gold/tests/conftest.py). Mirror that here so
# these tests import the real `src/silver/utils` package.
#
# IMPORTANT: run this suite in its own pytest invocation (e.g.
# `pytest src/silver/tests`), never combined with src/gold/tests in the
# same run -- see src/gold/tests/conftest.py for why.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
