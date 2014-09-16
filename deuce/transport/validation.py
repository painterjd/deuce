
import re
import inspect
from functools import wraps
from collections import namedtuple


from deuce.transport.wsgi import errors


VAULT_ID_MAX_LEN = 128
VAULT_ID_REGEX = re.compile('^[a-zA-Z0-9_\-]+$')
BLOCK_ID_REGEX = re.compile('\\b[0-9a-f]{40}\\b')
FILE_ID_REGEX = re.compile(
    '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')
OFFSET_REGEX = re.compile(
    '(?<![-.])\\b[0-9]+\\b(?!\\.[0-9])')
LIMIT_REGEX = re.compile(
    '(?<![-.])\\b[0-9]+\\b(?!\\.[0-9])')

ValidationRule = namedtuple('ValidationRule', 'vfunc errfunc getter')


class ValidationFailed(ValueError):

    """User input was inconsistent with API restrictions"""

    def __init__(self, msg, *args, **kwargs):
        msg = msg.format(*args, **kwargs)
        super(ValidationFailed, self).__init__(msg)


class ValidationProgrammingError(ValueError):

    """Caller did not map validations correctly"""

    def __init__(self, msg, *args, **kwargs):
        msg = msg.format(*args, **kwargs)
        super(ValidationProgrammingError, self).__init__(msg)


def Rule(vfunc, on_error, getter=None):
    """Constructs a single validation rule. A rule effectively
    is saying "I want to validation this input using
    this function and if validation fails I want this
    to happen.

    :param vfunc: The function used to validate this param
    :param on_error: The function to call when an error is detected
    :param value_src: The source from which the value can be
        This function should take a value as a field name
        as a single param.
    """
    return ValidationRule(vfunc=vfunc, errfunc=on_error, getter=getter)


def validate(**rules):
    """Falcon validation endpoint decorator

    This decorator allows validation of input from user
    API endpoints. This allows separation of business logic
    and avoids convoluted validation logic.

    Typical use is as follows:

    @validate({'bird_id': val_bird_id(allow_none=True), 404)
    def get_one(self, bird_id):
        lookup_bird_data(bird_id)

    In this example, bird_id will be passed to val_bird_id
    and validated. If the validation function throws the
    ValidationFailed exception, the specified code will be returned
    in the response and the resultant function will never actually
    be called.
    """

    def _validate(f):
        @wraps(f)
        def wrapper(*args, **kwargs):

            funcparams = inspect.getargspec(f)

            # Holds the list of validated values. Only
            # these values are passed to the endpoint
            outargs = dict()

            # Create dictionary that maps parameters passed
            # to their values passed
            param_values = dict(zip(funcparams.args, args))

            # Bring in kwargs so that we can validate as well
            param_values.update(kwargs)

            for param, rule in rules.items():

                # Where can we get the value? It's either
                # the getter on the rule or we default
                # to verifying parameters.
                getval = rule.getter or param_values.get

                # Call the validation function, passing
                # the value was retrieved from the getter
                try:
                    value = getval(param)

                    # Ensure that this validation function
                    # did not return a funciton. This
                    # checks that the user did not forget to
                    # execute the outer function of a closure
                    # in the rule declaration
                    resp = rule.vfunc(value)

                    if inspect.isfunction(resp):
                        msg = 'Validation function returned a function.'
                        raise ValidationProgrammingError(msg)

                    # If this is a param rule, add the
                    # param to the list of out args
                    if rule.getter is None:
                        outargs[param] = value

                except ValidationFailed as ex:
                    # TODO: log this validaiton failure

                    # TODO: should we call the error handler
                    # with the value that failed to validate?
                    rule.errfunc()
                    return

            return f(*args, **kwargs)
        return wrapper
    return _validate


def validation_function(func):
    """Decorator for creating a validation function"""
    @wraps(func)
    def inner(none_ok=False, empty_ok=False):
        def wrapper(value):
            if none_ok and value is None:
                return

            if not none_ok and value is None:
                msg = 'None value not permitted'
                raise ValidationFailed(msg)

            if empty_ok and value == '':
                return

            if not empty_ok and value == '':
                msg = 'Empty value not permitted'
                raise ValidationFailed(msg)
            func(value)
        return wrapper
    return inner


@validation_function
def val_vault_id(value):
    if not VAULT_ID_REGEX.match(value):
        raise ValidationFailed('Invalid vault id {0}'.format(value))

    if len(value) > VAULT_ID_MAX_LEN:
        raise ValidationFailed('Vault ID exceeded max len {0}'.format(
            VAULT_ID_MAX_LEN))


@validation_function
def val_block_id(value):
    if not BLOCK_ID_REGEX.match(value):
        raise ValidationFailed('Invalid Block ID {0}'.format(value))


@validation_function
def val_file_id(value):
    if not FILE_ID_REGEX.match(value):
        raise ValidationFailed('Invalid File ID {0}'.format(value))


@validation_function
def val_offset(value):
    if not OFFSET_REGEX.match(value):
        raise ValidationFailed('Invalid offset {0}'.format(value))


@validation_function
def val_limit(value):
    if not LIMIT_REGEX.match(value):
        raise ValidationFailed('Invalid limit {0}'.format(value))


@validation_function
def val_none_value(value):
    if value is not None:  # pragma: no cover
        msg = 'Value must be None'
        raise ValidationFailed(msg)


def _abort(status_code):
    if status_code == 400:
        raise errors.HTTPBadRequestAPI('Invalid Request')
    elif status_code == 404:
        raise errors.HTTPNotFound


# parameter rules
VaultGetRule = Rule(val_vault_id(), lambda: _abort(404))
VaultPutRule = Rule(val_vault_id(), lambda: _abort(400))
BlockGetRule = Rule(val_block_id(), lambda: _abort(404))
# BlockPostRule = Rule(val_vault_id(), lambda: _abort(400))
FileGetRule = Rule(val_file_id(), lambda: _abort(404))
FilePostRuleNoneOk = Rule(val_file_id(none_ok=True), lambda: _abort(400))
BlockPutRuleNoneOk = Rule(val_block_id(none_ok=True), lambda: _abort(400))
ReqNoneRule = Rule(val_none_value(none_ok=True), lambda: _abort(404))

# query string rules


def VaultMarkerRule(func):
    @wraps(func)
    def wrap(*args, **kwargs):
        vaultmarker = Rule(
            val_vault_id(
                none_ok=True),
            lambda: _abort(404),
            lambda v: args[1].get_param(v))

        @wraps(func)
        @validate(marker=vaultmarker)
        def validator(*args, **kwargs):
            return func(*args, **kwargs)
        validator(*args, **kwargs)
    return wrap


def FileMarkerRule(func):
    @wraps(func)
    def wrap(*args, **kwargs):
        filemarker = Rule(
            val_file_id(
                none_ok=True),
            lambda: _abort(404),
            lambda v: args[1].get_param(v))

        @wraps(func)
        @validate(marker=filemarker)
        def validator(*args, **kwargs):
            return func(*args, **kwargs)
        validator(*args, **kwargs)
    return wrap


def OffsetMarkerRule(func):
    @wraps(func)
    def wrap(*args, **kwargs):
        offsetmarker = Rule(
            val_offset(
                none_ok=True),
            lambda: _abort(404),
            lambda v: args[1].get_param(v))

        @wraps(func)
        @validate(marker=offsetmarker)
        def validator(*args, **kwargs):
            return func(*args, **kwargs)
        validator(*args, **kwargs)
    return wrap


def BlockMarkerRule(func):
    @wraps(func)
    def wrap(*args, **kwargs):
        blockmarker = Rule(
            val_block_id(
                none_ok=True),
            lambda: _abort(404),
            lambda v: args[1].get_param(v))

        @validate(marker=blockmarker)
        def validator(*args, **kwargs):
            return func(*args, **kwargs)
        validator(*args, **kwargs)
    return wrap


def LimitRule(func):
    @wraps(func)
    def wrap(*args, **kwargs):
        limitrule = Rule(
            val_limit(
                none_ok=True),
            lambda: _abort(404),
            lambda v: args[1].get_param(v))

        @validate(limit=limitrule)
        def validator(*args, **kwargs):
            return func(*args, **kwargs)
        validator(*args, **kwargs)
    return wrap
