#!/bin/bash

API_TEST_ENV_NAME="env_api_tests"
PYTHON_EXE=python2
TESTING_HOME=~/.deuce
TESTING_CONFIG_FILE=${TESTING_HOME}/tests.conf
TESTING_LOGS=${TESTING_HOME}/logs
TESTING_ROOT_LOGS=${TESTING_LOGS}/root
TESTING_TEST_LOGS=${TESTING_LOGS}/tests

DEUCE_API_TESTS_SAMPLE_TEST_CONFIG=tests/etc/tests.conf.sample
DEUCE_API_TESTS_REQUIREMENTS=tests/test-requirements.txt


setupEnv()
	{
	if [ -d ${API_TEST_ENV_NAME} ]; then
		rm -Rf ${API_TEST_ENV_NAME}
	fi
	virtualenv -p `which ${PYTHON_EXE}` ${API_TEST_ENV_NAME}
	source ${API_TEST_ENV_NAME}/bin/activate
	pip install -r ${DEUCE_API_TESTS_REQUIREMENTS}
	}

setupTestingConfiguration()
	{
	echo -n "Checking for ${TESTING_HOME}..."
	if [ -d ${TESTING_HOME} ]; then
		echo "exists"
	else
		echo "creating"
		mkdir ${TESTING_HOME}
	fi

	echo -n "Checking for ${TESTING_CONFIG_FILE}..."
	if [ -f ${TESTING_CONFIG_FILE} ]; then
		echo "exists"
		echo "	Assuming configured."
	else
		echo "creating"
		cp ${DEUCE_API_TESTS_SAMPLE_TEST_CONFIG} ${TESTING_CONFIG_FILE}
		echo "Please configure ${TESTING_CONFIG_FILE}"
		exit 1
	fi

	echo -n "Checking for ${TESTING_LOGS}..."
	if [ -d ${TESTING_LOGS} ]; then
		echo "exists"
	else
		echo "creating"
		mkdir ${TESTING_LOGS}
	fi

	echo -n "Checking for ${TESTING_ROOT_LOGS}"
	if [ -d ${TESTING_ROOT_LOGS} ]; then
		echo "exists"
	else
		echo "creating"
		mkdir ${TESTING_ROOT_LOGS}
	fi

	echo -n "Checking for ${TESTING_TEST_LOGS}"
	if [ -d ${TESTING_TEST_LOGS} ]; then
		echo "exists"
	else
		echo "creating"
		mkdir ${TESTING_TEST_LOGS}
	fi
	}

runTests()
	{
	pushd tests
		export CAFE_CONFIG_FILE_PATH=${TESTING_CONFIG_FILE}
		export CAFE_ROOT_LOG_PATH=${TESTING_ROOT_LOGS}
		export CAFE_TEST_LOG_PATH=${TESTING_TEST_LOGS}
		nosetests --with-xunit --nologcapture api
	popd
	}

setupEnv
setupTestingConfiguration
runTests
