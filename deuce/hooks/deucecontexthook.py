"""
Deuce Context Hook
"""
from pecan.hooks import PecanHook
from pecan.core import abort

import deuce


def initialize_deuce_context(headers):
    """
    Initialize the Deuce Context based on the provided headers
    """
    from threading import local as local_factory
    deuce.context = local_factory()


class DeuceContextHook(PecanHook):
    """Initialize the Deuce Context based on the Pecan Request"""

    def on_route(self, state):
        initialize_deuce_context(state.request.headers)
