import deuce


class Health(object):

    @staticmethod
    def health():
        return deuce.metadata_driver.get_health()
