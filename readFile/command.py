import pandas as pd
from otlang.sdk.syntax import Keyword, Positional, OTLType
from pp_exec_env.base_command import BaseCommand, Syntax
from df_storage import DfStorage, get_read_write_args


class ReadfileCommand(BaseCommand):
    # define syntax of your command here
    syntax = Syntax(
        [
            Positional("filename", required=False, otl_type=OTLType.TEXT),
            Keyword("type", required=False, otl_type=OTLType.TEXT),
            Keyword("storage", required=False, otl_type=OTLType.TEXT),
            Keyword("private", required=False, otl_type=OTLType.BOOLEAN),
            Keyword("path", required=False, otl_type=OTLType.TEXT),
            Keyword("format", required=False, otl_type=OTLType.TEXT)
        ],
    )
    use_timewindow = False  # Does not require time window arguments
    idempotent = True  # Does not invalidate cache

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

        df_storage = DfStorage(
            storage_path,
            user_id=self.platform_envs['user_guid'],
            private=self.get_arg('private').value
        )

        df = df_storage.read(
            filename,
            file_type
        )
        return df
