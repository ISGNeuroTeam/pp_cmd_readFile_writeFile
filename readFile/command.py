import pandas as pd
from pathlib import Path
from otlang.sdk.syntax import Keyword, Positional, OTLType
from pp_exec_env.base_command import BaseCommand, Syntax
from df_storage import DfStorage


class ReadfileCommand(BaseCommand):
    # define syntax of your command here
    syntax = Syntax(
        [
            Positional("filename", required=True, otl_type=OTLType.TEXT),
            Keyword("type", required=False, otl_type=OTLType.TEXT),
            Keyword("storage", required=False, otl_type=OTLType.TEXT),
            Keyword("private", required=False, otl_type=OTLType.BOOLEAN)
        ],
    )
    use_timewindow = False  # Does not require time window arguments
    idempotent = True  # Does not invalidate cache

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:

        if 'storages' in self.config:
            storages = self.config['storages']
        else:
            storages = {
                'lookups': '/opt/otp/lookups'
            }

        storage = self.get_arg('storage').value or 'lookups'
        if storage not in storages:
            raise ValueError('Unknown storage')

        Path(storage).mkdir(exist_ok=True, parents=True)
        df_storage = DfStorage(
            storages[storage],
            user_id=self.platform_envs['user_guid'],
            private=self.get_arg('private').value
        )

        df = df_storage.read(
            self.get_arg('filename').value,
            self.get_arg('type').value
        )
        return df
