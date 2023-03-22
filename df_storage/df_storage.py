import os
import pathlib

import pandas as pd

from enum import Enum
from pathlib import Path

class WriteMode(Enum):
    OVERWRITE = 'overwrite'
    APPEND = 'append'
    IGNORE = 'ignore'

class DfStorage:
    def __init__(self, storage_dir: str, user_id: str, private: bool = False):
        self.storage_dir = Path(storage_dir)
        self.user_id = str(user_id)
        self.private = private

        if self.user_id is None and self.private:
            raise ValueError('Private saving need user_id')

        if not self.storage_dir.exists():
            self.storage_dir.mkdir(parents=True)

    def write(self, df: pd.DataFrame, df_path: str, file_type=None, mode: WriteMode = WriteMode.OVERWRITE):

        path_in_storage, df_name = self._get_path_in_storage_and_name(df_path)

        if self.private:
            df_storage_dir = self._get_private_storage_path() / path_in_storage
        else:
            df_storage_dir = self._get_public_storage_path() / path_in_storage

        if not df_storage_dir.exists():
            df_storage_dir.mkdir(parents=True, exist_ok=True)

        full_df_path = df_storage_dir / df_name
        self._save_pandas_df(df, full_df_path, file_type, mode)

    def _save_pandas_df(self, df, full_df_path: Path, file_type=None, mode: WriteMode= WriteMode.OVERWRITE):
        if full_df_path.exists() and mode == WriteMode.IGNORE:
            return

        file_type = self._get_file_type(full_df_path, file_type)
        if file_type == 'parquet':
            self._save_pandas_df_as_parquet(df, full_df_path, mode)
        elif file_type == 'json':
            self._save_pandas_df_as_json(df, full_df_path, mode)
        elif file_type == 'csv':
            self._save_pandas_df_as_csv(df, full_df_path, mode)
        else:
            raise ValueError('Unknown type')

    @staticmethod
    def _save_pandas_df_as_csv(df: pd.DataFrame, full_df_path: Path, mode: WriteMode):
        if mode == WriteMode.OVERWRITE:
            df.to_csv(
                full_df_path, index=False
            )
        elif mode == WriteMode.APPEND:
            df.to_csv(
                full_df_path, mode='a', header=None, index=False
            )
        else:
            raise ValueError('Unsupported write mode')


    @staticmethod
    def _save_pandas_df_as_json(df: pd.DataFrame, full_df_path: Path, mode: WriteMode):
        if mode == WriteMode.APPEND:
            with open(full_df_path, mode='a') as f:
                f.write(
                    df.to_json(
                        orient='records', lines=True
                    )
                )
        elif mode == WriteMode.OVERWRITE:
            df.to_json(
                full_df_path, orient='records', lines=True
            )
        else:
            raise ValueError('Unsupported write mode type')
    @staticmethod
    def _save_pandas_df_as_parquet(df: pd.DataFrame, full_df_path: Path, mode: WriteMode):
        if mode == WriteMode.OVERWRITE:
            df.to_parquet(
                full_df_path, engine='pyarrow', compression=None
            )
        elif mode == WriteMode.APPEND:
            df.to_parquet(full_df_path, engine='fastparquet', compression=None, append=True)
        else:
            raise ValueError('Usupported writefile mode')

    def _read_pandas_df(self, full_df_path, file_type=None) -> pd.DataFrame:
        file_type = self._get_file_type(full_df_path, file_type)

        # check file exists
        if not full_df_path.exists():
            raise ValueError('File not exists')
        if file_type == 'json':
            df = pd.read_json(full_df_path, lines=True)
        elif file_type == 'parquet':
            df = pd.read_parquet(
                full_df_path, engine='pyarrow', use_pandas_metadata=True
            )
        elif file_type == 'csv':
            df = pd.read_csv(full_df_path)
        else:
            raise ValueError('Unknown type')
        return df

    @staticmethod
    def _get_file_type(file_name, file_type=None):
        file_type = file_type or str(file_name).split('.')[-1]
        return file_type

    def read(self, df_path: str, file_type=None):
        path_in_storage, df_name = self._get_path_in_storage_and_name(df_path)
        private_df_full_path = self._get_private_storage_path() / path_in_storage / df_name
        public_df_full_path = self._get_public_storage_path() / path_in_storage / df_name

        if public_df_full_path.exists() and not self.private:
            full_df_path = public_df_full_path
        elif self.user_id and private_df_full_path.exists():
            full_df_path = private_df_full_path
        else:
            raise ValueError(f'File with path {df_path} not found')

        return self._read_pandas_df(full_df_path, file_type)

    def list(self, user_id):
        """
        Returns all dfs names from public storage and private if private=True
        """
        public_storage = self._get_public_storage_path()
        public_df_list = [
            (os.path.join(dp, f).replace(str(public_storage) + os.sep, ''), 'public')
            for dp, dn, filenames in os.walk(public_storage) for f in filenames
        ]
        private_storage = self._get_private_storage_path()
        private_df_list = [
            (os.path.join(dp, f).replace(str(private_storage) + os.sep, ''), 'private')
            for dp, dn, filenames in os.walk(private_storage) for f in filenames
        ]
        return public_df_list + private_df_list

    def _get_private_storage_path(self):
        return self.storage_dir / self.user_id

    def _get_public_storage_path(self):
        return self.storage_dir

    def delete(self, df_path: str, private=False):
        path_in_storage, df_name = self._get_path_in_storage_and_name(df_path)
        private_df_full_path = self._get_private_storage_path() / path_in_storage / df_name
        public_df_full_path = self._get_public_storage_path() / path_in_storage / df_name
        if private:
            private_df_full_path.unlink(missing_ok=True)
        else:
            public_df_full_path.unlink(missing_ok=True)

    @staticmethod
    def _get_path_in_storage_and_name(df_path: str):
        df_path = Path(df_path)
        df_name = df_path.name
        path_in_storage = str(df_path.parent)
        if path_in_storage[0] == os.sep:
            path_in_storage = path_in_storage[1:]
        return path_in_storage, df_name
