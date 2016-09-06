import pickle
from os import makedirs
from threading import Lock

from niocore.modules.persistence.file.resolver import FilePersistenceResolver
from niocore.util.codec import load_pickle
from nio.util.logging import get_nio_logger
from niocore.util.environment import NIOEnvironment


class Persistence(object):

    """ Persistence Module

    This class encapsulates the user-facing interface to NIO's
    persistence layer. Block writers use this module to save dynamic
    data at runtime, allowing it to persist in the block instance
    after a service/instance restart.

    Persistence data is namespaced on a service-by-service basis. That is,
    blocks with the same name in different services can persist dynamic
    data independently.

    Args:
        name (str): The name of the block whose data will be persisted.

    Example:
        self.persistence.store('foo', 'bar')
        self.persistence.save() # saves the stored k/v store to disk
        val = self.persistence.load('foo') # now val == 'bar'

    """

    _service = ''
    _target = None
    _file_lock = Lock()

    def __init__(self, name):
        """ Constructor for the Persistence module """
        self._resolver = FilePersistenceResolver(self._target)
        self._name = name
        self._filename = self._resolver.resolve(self._service)
        self._values = {}
        self._cleared = []
        self.logger = get_nio_logger("NIOPersistence")

    def store(self, key, value):
        """ Store a key/value datum in *memory*. This does *not* save the datum
        to disk. Users of this module must explicitly call Persistence.save to
        initiate disk I/O.

        Args:
            key: The key to store into
            value: The value associated with that key

        Returns:
            None

        """
        self._values = self._values or self._load_data()
        self._values[key] = value

    def load(self, key, default=None):
        """ Load a value from the k/v store *currently in memory*.
        The in-memory store is updated from disk only the first time
        load is called. However, all changes that occur during a
        service run are reflected in the in-mem store, so periodic
        updates are unnecessary.

        Args:
            key: The key to lookup in the store.
            default: the value to return if the key is not present

        Returns:
            value: The value associated with that key

        """
        self._values = self._values or self._load_data()
        return self._values.get(key, default)

    def has_key(self, key):
        """ Check whether a particular key exists in the persistence
        key/val store.

        Args:
            key: The key in question.

        Returns:
            exists (bool)

        """
        self._values = self._values or self._load_data()
        return key in self._values

    def clear(self, key):
        """ Remove the given key and associated value from the in-mem
        k/v store. As above, this will not be reflected in the on-disk
        store until Persistence.save is called.

        Args:
            key: The key pointing to the data to clear.

        """
        if self._values.get(key) is not None:
            del self._values[key]
        self._cleared.append(key)

    def save(self):
        """ Save the in-memory store to disk. This allows the data therein
        to persist between instance/service restarts.

        Returns:
            None

        """
        service_data = self._load_pickle_file(self._filename)

        block_data = service_data.get(self._name, {})

        # delete all cleared keys from the block data about to be saved
        block_data = self._remove_cleared_keys(block_data)

        service_data[self._name] = dict(
            list(block_data.items()) + list(self._values.items())
        )
        with self._file_lock:
            f = open(self._filename, 'wb+')
            pickle.dump(service_data, f)
            f.close()

    def _load_data(self):
        """ Private method to read the component-specific store into memory
        from the persistent one on disk.

        Returns:
            store (dict): The k/v store associated with this particular
                          block within a service.

        """
        data = self._load_pickle_file(self._filename)
        return data.get(self._name, {})

    def _load_pickle_file(self, filename):
        """Load a pickled file into a dictionary

        Args:
            filename (str): The filename where the pickle was saved

        Returns:
            out (dict): Dictionary of loaded file. If there was an error, the
            message will be logged and an empty dict is returned.
        """
        data = {}
        try:
            with self._file_lock:
                data = load_pickle(filename)
        except Exception as e:  # pragma: no cover
            self.logger.exception(
                "Failed to parse Pickle file {0} : {1}".format(
                    filename, type(e).__name__))
        return data

    def _remove_cleared_keys(self, block_data):
        """" Helper method to remove cleared keys from the block data
        before storing it back out to disk.

        This allows us to work in memory, loading the persistent store
        from disk only on a 'save' request.

        """
        for key in self._cleared:
            if block_data.get(key) is not None:
                del block_data[key]
        return block_data

    @classmethod
    def setup(cls, context):
        """ Set up the persistence - this will be called before proxying.

        This method is called once in each process by the module implementation
        and is expected to set the information for the service's persistence to
        the class. As a result, this method must be called before the
        implementation is proxied, since it makes use of cls which will always
        be the implementation.
        """
        cls._service = context.service_name
        cls._target = NIOEnvironment.get_path(context.data)
        try:
            makedirs(cls._target)
        except OSError:
            # If the persistence target directory already exists, move on
            pass
