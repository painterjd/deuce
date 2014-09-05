
import deuce

from deuce.model import Health
from deuce.util import log as logging
logger = logging.getLogger(__name__)


class HealthController(object):

    @staticmethod
    def get_health():
        return Health.health()
