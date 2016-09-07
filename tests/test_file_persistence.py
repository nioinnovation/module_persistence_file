import os
import glob
from ..module import FilePersistenceModule
from nio.modules.persistence import Persistence
from nio.modules.context import ModuleContext
from niocore.testing.service_test_case import NIOServiceTestCase
from nio.block.base import Block
from nio.service.base import BlockExecution


class Dummy(Block):

    def __init__(self):
        super().__init__()

    def start(self):
        super().start()

    def configure(self, context):
        super().configure(context)
        self.persistence = Persistence('Dummy')

    def process_signals(self, signals):
        self.foo = self.persistence.load('foo') or 0
        self.foo += 1
        self.persistence.store('foo', self.foo)
        self.persistence.save()


blocks = [{"type": Dummy,
           "properties": {'name': 'persister_block'}},
          {"type": Dummy,
           "properties": {'name': 'persister_block1'}}]

execution = [BlockExecution()]
execution[0].name = "persister_block"
execution[0].receivers = []

mappings = [{"name": 'persister_block1',
             "mapping": 'persister_block'}]

properties = {"name": "PersistenceTestInstance",
              "auto_start": False,
              "execution": execution,
              "mappings": mappings}


class TestFilePersistence(NIOServiceTestCase):

    cfg_dir = "{}/{}/".format(os.path.dirname(__file__), "persist_test")

    def setUp(self):
        try:
            os.mkdir(self.cfg_dir)
        except FileExistsError:  # pragma: no cover
            # No problem, the directory already exists
            pass

        # set up here after doing all of the file stuff
        # this will proxy the modules and set them up
        super().setUp()
        # Make sure the module has the proper class variables
        self.assertIsNotNone(Persistence._target)
        # Make sure after the service is configured that our persistence
        # class contains the proper service name
        self.assertEqual(Persistence._service, 'PersistenceTestInstance')

    def get_service_blocks(self):
        return blocks

    def get_service_properties(self):
        return properties

    def get_context(self, module_name, module):
        if module_name == "persistence":
            context = ModuleContext()
            context.data = self.cfg_dir
            context.service_name = 'PersistenceTestInstance'
            return context
        else:
            return super().get_context(module_name, module)

    def get_module(self, module_name):
        """ Override to use the file persistence """
        if module_name == "persistence":
            return FilePersistenceModule()
        else:
            return super().get_module(module_name)

    def get_test_modules(self):
        return super().get_test_modules() | {'persistence'}

    def tearDown(self):
        super().tearDown()

        # Remove any files we used
        for f in glob.glob("%s/*.*" % self.cfg_dir):
            os.remove(f)
        os.rmdir(self.cfg_dir)

    def test_persistence(self):
        # ensure that the value of 'foo' persists between service
        # runs.
        self.service.start()
        self.service._blocks['persister_block'].process_signals([])
        self.service.stop()

        self.service.start()
        self.service._blocks['persister_block'].process_signals([])
        self.assertEqual(self.service._blocks['persister_block'].foo, 2)
        self.service.stop()

    def test_persistence_independence(self):
        self.service.start()

        # ensure that two instances of the block can persist their
        # data independently.

        self.service._blocks['persister_block'].process_signals([])
        self.service._blocks['persister_block'].process_signals([])
        self.assertEqual(self.service._blocks['persister_block'].foo, 2)

        self.service._blocks['persister_block'].persistence.clear('foo')
        self.service._blocks['persister_block'].persistence.save()

        self.service._blocks['persister_block1'].process_signals([])

        # The 'foo' value from the other Dummy instance should not get
        # picked up by this one.
        self.assertEqual(
            self.service._blocks['persister_block1'].foo, 1
        )

        self.service.stop()

    def test_persistence_clear(self):
        self.service.start()
        self.service._blocks['persister_block'].process_signals([])
        self.assertEqual(self.service._blocks['persister_block'].foo, 1)
        self.service.stop()

        self.service.start()
        self.service._blocks['persister_block'].persistence.clear('foo')
        self.service._blocks['persister_block'].persistence.save()
        self.service.stop()

        self.service.start()
        self.service._blocks['persister_block'].process_signals([])
        self.assertEqual(self.service._blocks['persister_block'].foo, 1)
        self.service.stop()

    def test_key_exists(self):
        self.service.start()
        persister = self.service._blocks['persister_block']
        persister.persistence.store('foo', 3)
        val = persister.persistence.load('foo')
        self.assertEqual(val, 3)

        exists = persister.persistence.has_key('foo')  # nopep8
        self.assertTrue(exists)
        self.service.stop()

    def test_nonexistent_key(self):
        self.service.start()
        persister = self.service._blocks['persister_block']
        val = persister.persistence.load('bar')
        self.assertIsNone(val)

        exists = persister.persistence.has_key('bar')  # nopep8
        self.assertFalse(exists)
        self.service.stop()
