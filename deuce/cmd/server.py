from deuce.common import cli
from deuce.transport.wsgi.driver import Driver


@cli.runnable
def run():
    app_container = Driver()
    app_container.listen()
