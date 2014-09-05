=============================================
Swift/Cloud Files Driver Testing Instructions
=============================================

1) In config.py, replace swift_module, is_mocking, username, password, auth_url, and storage_url with the correct values.

Before:

.. code-block:: python

    block_storage_driver = {
        # ...
        'swift': {
            # ...
            'testing': {
                'is_mocking': True
                'username': 'User name',
                'password': 'Password',
                'auth_url': 'Auth Url',
                'storage_url': 'Storage Url'
            }
        }
    }


After:

.. code-block:: python

    block_storage_driver = {
        # ...
        'swift': {
            # ...
            'testing': {
                'is_mocking': False
                'username': 'User name',
                'password': 'Password',
                'auth_url': 'https://identity.api.rackspacecloud.com/v2.0/',
                'storage_url': 'Storage Url'
            }
        }
    }


3) Run tests with the storage in Rackspace Cloud Files.

4) If tests fail with driver.delete_vault(), the problematic vault and the garbage inside it need to be deleted manually from the control panel. 
