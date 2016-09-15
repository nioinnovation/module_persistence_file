import os
from nio.modules.context import ModuleContext
from nio.modules.persistence.module import PersistenceModule
from nio.modules.settings import Settings
from nio import discoverable
from niocore.util.environment import NIOEnvironment

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
        context.root_folder = self._get_abspath_folder(Settings.get(
            'persistence', 'configuration_data', fallback='etc'))
        # save/load files as json
        context.format = Persistence.Format.json.value
        return context

    def prepare_service_context(self, service_context=None):
        context = ModuleContext()
        # prefix files with service name to avoid collisions when same block
        # is used from different services
        context.root_id = service_context.properties['name']
        # specify the root path for block persistence files
        context.root_folder = self._get_abspath_folder(Settings.get(
            'persistence', 'data', fallback='etc/persist'))
        # save/load block persisted files as pickle
        context.format = Persistence.Format.pickle.value
        return context

    @staticmethod
    def _get_abspath_folder(path):
        """ Converts a relative path to absolute using NIOEnvironment

        When passing a relative path it gets converted to absolute, if the
        path is already absolute same path is returned

        Args:
            path (str): relative path

        Returns:
            absolute path
        """
        if os.path.isabs(path):
            return path
        return NIOEnvironment.get_path(path)
