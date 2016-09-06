from nio.modules.context import ModuleContext
from nio.modules.persistence.module import PersistenceModule
from nio.modules.settings import Settings
from niocore.modules.persistence.file import Persistence
from nio import discoverable


@discoverable
class FilePersistenceModule(PersistenceModule):

    def initialize(self, context):
        super().initialize(context)
        # Set up the implementation class vars before proxying
        Persistence.setup(context)
        self.proxy_persistence_class(Persistence)

    def finalize(self):
        super().finalize()

    def _prepare_common_context(self, service_name):
        context = ModuleContext()
        context.data = Settings.get(
            'persistence', 'data', fallback='etc/persist')
        context.service_name = service_name
        return context

    def prepare_core_context(self):
        return self._prepare_common_context('main')

    def prepare_service_context(self, service_context):
        return self._prepare_common_context(service_context.properties['name'])
