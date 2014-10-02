import deuce.util.log as logging
from deuce.transport.wsgi.driver import Driver

app_container = Driver()
logging.setup()
app = app_container.app
