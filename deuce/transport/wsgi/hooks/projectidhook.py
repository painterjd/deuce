import deuce
from deuce.transport.wsgi.hooks import healthpingcheck


@healthpingcheck
def ProjectidHook(req, resp, params):
    deuce.context.project_id = req.get_header('x-project-id', required=True)
