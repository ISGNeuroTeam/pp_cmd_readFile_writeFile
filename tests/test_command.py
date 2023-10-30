"""Module for testing {{command_name}} command"""
import os
import pandas as pd
from unittest import TestCase
from sys import prefix

from postprocessing_sdk.commands.pp import Command

from otlang.exceptions import OTLException

class TestCommand(TestCase):
    """Class for testing tail command"""

    def setUp(self):
        self.command = Command()
        commands_dir = os.path.join(prefix,
                                    'lib/python3.9/site-packages/postprocessing_sdk/pp_cmd')
        self.command._create_command_executor(storage='', commands_dir=commands_dir)
        parent_dir = os.path.dirname(os.path.dirname(__file__))
        # add current command to self.command with _import_user_commands()
        self.command.command_executor.command_classes.update(
            self.command.command_executor._import_user_commands(commands_directory=parent_dir, follow_links=True))

    def run_otl(self, otl_query: str = '') -> pd.DataFrame:
        return self.command.run_otl(otl_query=otl_query, storage='', df_print=False)

    def test_readfile_command(self):
        # enter sample dataframe
        sample = sample = pd.DataFrame([
            [2145, 1],
            [654372, 2],
            [46, 3],
            [35678, 4],
            [865476, 5],
            [435378, 6],
            [8647, 7],
            [-418084, 42367],
            [-844815, 2],
            [-1271546, 3]
        ], columns=["a", "b"])
        otl_query = '| readFile example_002.csv type=csv storage=lookups'
        result = self.run_otl(otl_query=otl_query)
        # check if sample and result are the same
        pd.testing.assert_frame_equal(sample, result)

    def test_writefile_command(self):
        # enter sample dataframe
        sample = pd.DataFrame()
        # create otl query that should return the same dataframe as you have in sample
        otl_query = ''
        # calculate otl query with postprocessing
        result = self.run_otl(otl_query=otl_query)
        # check if sample and result are the same
        pd.testing.assert_frame_equal(sample, result)


