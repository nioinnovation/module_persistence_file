from nio.modules.context import ModuleContext
from nio.modules.persistence.module import PersistenceModule
from nio.modules.settings import Settings
from nio import discoverable
from niocore.util.environment import NIOEnvironment
from os.path import isabs, normpath

from . import Persistence


@discoverable
class FilePersistenceModule(PersistenceModule):

    def initialize(self, context):
        super().initialize(context)
        # Set up the implementation class vars before proxying
        Persistence.configure(context)
        self.proxy_persistence_class(Persistence)

    def finalize(self):
        super().finalize()

    def prepare_core_context(self):
        context = ModuleContext()
        # no need to prefix files, configuration files will reside under
        # 'blocks' and 'services' collections
        context.root_id = ''
        # specify the root path for 'blocks' and 'services'
        # getting absolute path through nio to so that it gets prefixed with
        # the project path and not the current path, which could even raise
        # an exception if the current folder has been deleted
        context.root_folder = self._get_abspath(Settings.get(
            'persistence', 'configuration_data', fallback="etc"))
        # save/load files as json
        context.format = Persistence.Format.json.value
        return context

    def prepare_service_context(self, service_context=None):
        context = ModuleContext()
        # prefix files with service name to avoid collisions when same block
        # is used from different services
        context.root_id = service_context.properties['name']
        # specify the root path for block persistence files
        # getting absolute path through nio to so that it gets prefixed with
        # the project path and not the current path, which could even raise
        # an exception if the current folder has been deleted
        context.root_folder = self._get_abspath(Settings.get(
            'persistence', 'data', fallback='etc/persist'))
        # save/load block persisted files as pickle
        context.format = Persistence.Format.pickle.value
        return context

    def _get_abspath(self, path):
        """ Produces an absolute path using nio project's path

        If path parameter is relative, the returned path becomes absolute by
        prefixing it with NIO project's path

        Args:
            path (str): path to process

        Returns:
             absolute path
        """
        if not isabs(path):
            return NIOEnvironment.get_path(path)

        return normpath(path)
