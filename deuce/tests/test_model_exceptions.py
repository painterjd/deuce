import deuce
from deuce.tests import V1Base

from deuce.model.exceptions import ConsistencyError


class TestModelExceptions(V1Base):

    def setUp(self):
        super(TestModelExceptions, self).setUp()

    def test_consistency_error_with_msg(self):
        try:
            raise ConsistencyError(deuce.context.project_id,
                                   self.create_vault_id(),
                                   self.create_block_id(b'mock'),
                                   msg='additional context')
        except ConsistencyError as ex:
            self.assertIn('additional context', str(ex))

    def test_consistency_error_without_msg(self):
        with self.assertRaises(ConsistencyError):
            raise ConsistencyError(deuce.context.project_id,
                                   self.create_vault_id(),
                                   self.create_block_id(b'mock'))
