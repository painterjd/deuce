import deuce

from deuce.common import context
from deuce.common import local


def TransactionidHook(req, resp, params):
    transaction = context.RequestContext()
    setattr(local.store, 'context', transaction)
    deuce.context.transaction = transaction
    resp.set_header('Transaction-ID', deuce.context.transaction.request_id)
