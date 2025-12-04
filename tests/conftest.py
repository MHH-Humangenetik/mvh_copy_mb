"""
Pytest configuration and fixtures for the test suite.
"""
from hypothesis import settings

# Configure Hypothesis settings for all tests
# Disable deadline to avoid flaky failures during parallel execution
settings.register_profile("default", deadline=None)
settings.load_profile("default")
