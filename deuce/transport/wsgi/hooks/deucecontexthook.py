import deuce


def DeuceContextHook(req, resp, params):
    """
    Deuce Context Hook
    """
    from threading import local as local_factory
    deuce.context = local_factory()

    deuce.context.datacenter = deuce.conf.api_configuration.datacenter.lower()
