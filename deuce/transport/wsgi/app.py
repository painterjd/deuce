from deuce.transport.wsgi import Driver

app_container = Driver()

app = app_container.app
