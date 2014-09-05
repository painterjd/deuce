from cafe.engine.models.data_interfaces import ConfigSectionInterface


class deuceConfig(ConfigSectionInterface):
    """
    Defines the config values for deuce
    """
    SECTION_NAME = 'deuce'

    @property
    def base_url(self):
        """
        deuce endpoint
        """
        return self.get('base_url')

    @property
    def use_auth(self):
        """
        authenticate the user connection
        """
        return self.get_boolean('use_auth')

    @property
    def version(self):
        """
        deuce api version
        """
        return self.get('version')


class authConfig(ConfigSectionInterface):
    """
    Defines the auth config values
    """
    SECTION_NAME = 'auth'

    @property
    def base_url(self):
        """
        auth endpoint
        """
        return self.get('base_url')

    @property
    def user_name(self):
        """
        user account
        """
        return self.get('user_name')

    @property
    def api_key(self):
        """
        user's api key
        """
        return self.get('api_key')


class storageConfig(ConfigSectionInterface):
    """
    Defines the storage config values
    """
    SECTION_NAME = 'storage'

    @property
    def internal_url(self):
        """
        boolean indicating whether to use the internal storage url
        """
        return self.get_boolean('internal_url')

    @property
    def region(self):
        """
        storage url region
        """
        return self.get('region')
