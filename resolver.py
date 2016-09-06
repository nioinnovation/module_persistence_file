from niocore.util.resolver import FileResolver
from niocore.util.attribute_dict import AttributeDict


class FilePersistenceResolver(FileResolver):
    """ Resolver for gathering persistence files.

    """
    def __init__(self, target):
        settings = AttributeDict({})
        if target is not None:
            settings.data = target
        super().__init__(settings, conf_key='data', ext='.dat')
