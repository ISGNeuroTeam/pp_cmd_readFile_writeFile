import pandas as pd
from pathlib import Path

from otlang.sdk.syntax import Keyword, Positional, OTLType
from pp_exec_env.base_command import BaseCommand, Syntax
from df_storage import DfStorage, WriteMode, get_read_write_args


class WritefileCommand(BaseCommand):
    # define syntax of your command here
    syntax = Syntax(
        [
            Positional("filename", required=False, otl_type=OTLType.TEXT),
            Keyword("type", required=False, otl_type=OTLType.TEXT),
            Keyword("storage", required=False, otl_type=OTLType.TEXT),
            Keyword("private", required=False, otl_type=OTLType.BOOLEAN),
            Keyword("mode", required=False, otl_type=OTLType.TEXT),
            Keyword("path", required=False, otl_type=OTLType.TEXT),
            Keyword("format", required=False, otl_type=OTLType.TEXT)
        ],
    )

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # support spark readFile writeFile syntax
        # and if spark syntax then default storage external data
        storage_path, filename, file_type = get_read_write_args(
            self.config,
            self.get_arg('path').value,
            self.get_arg('storage').value,
            self.get_arg('filename').value,
            self.get_arg('type').value,
            self.get_arg('format').value
        )

        mode = WriteMode(self.get_arg('mode').value or 'overwrite')

        df_storage = DfStorage(
            storage_path,
            user_id=self.platform_envs['user_guid'],
            private=self.get_arg('private').value
        )

        df_storage.write(
            df,
            filename,
            file_type,
            mode
        )
        return df
