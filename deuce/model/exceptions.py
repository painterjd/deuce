'''

This file contains exceptions that can be thrown by the model, which
will in turn be caught by the endpoint resource and converted into an
appropriate status code

'''


class ConsistencyError(Exception):

    """ConsistencyError is raised when there is disagreement
    between the metadata and storage layer on the existence of a
    block in a given vault"""

    def __init__(self, project_id, vault_id, block_id, msg=None):
        """Creates a new ConsistencyError Exception
        :param project_id: The project ID under which the vault
        is housed
        :param vault_id: The vault containing the block
        :param block_id: The ID of the block in question
        :param msg: additional contextual information
        """
        self.project_id = project_id
        self.vault_id = vault_id
        self.block_id = block_id

        extended_msg = "[{0}/{1}] Consistency Error: [{2}] Block id: {3}"\
                       .format(project_id, vault_id, msg, block_id)

        Exception.__init__(self, extended_msg)
