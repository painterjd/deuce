Deuce API Tests
===============

To run the tests:

#) pip install -r *deuce/tests/test-requirements.txt* . The requirements for the api tests include:

   - nosetests
   - ddt
   - jsonschema
   - opencafe

#) Make a copy of *deuce/tests/etc/tests.conf.sample* and edit it with the appropriate information for the deuce base url of the environment being tested.
#) Set up the following environment variables:

   - **CAFE_CONFIG_FILE_PATH** (points to the complete path to your test configuration file) 

     ``export CAFE_CONFIG_FILE_PATH = ~/.project/tests.conf``
   - **CAFE_ROOT_LOG_PATH** (points to the location of your test logs) 

     ``export CAFE_ROOT_LOG_PATH=~/.project/logs``
   - **CAFE_TEST_LOG_PATH** (points to the location of your test logs) 

     ``export CAFE_TEST_LOG_PATH=~/.project/logs``

#) Once you are ready to execute the API tests:

   ``cd deuce/tests``

   ``nosetests --with-xunit --nologcapture api``

*Note*: You may want to run API Tests in parallel mode:

   ``nosetests --nologcapture --processes=8 --process-timeout=120 --process-restartworker api``


