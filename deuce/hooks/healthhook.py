from pecan.hooks import PecanHook


class HealthHook(PecanHook):
    def health(self, state):
        if hasattr(state.request, 'path') and \
                (state.request.path == '/v1.0/health' or
                state.request.path == '/v1.0/ping'):
            return True
