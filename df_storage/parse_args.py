from pathlib import Path


def get_read_write_args(
        command_config, path_arg, storage_arg, filename_arg, type_arg, format_arg
):
    if 'storages' in command_config:
        storages = command_config['storages']
    else:
        storages = {}

    # set default storages config
    if 'lookups' not in storages:
        storages['lookups'] = '/opt/otp/lookups'
    if 'external_data' not in storages:
        storages['external_data'] = '/opt/otp/external_data'

    if path_arg and filename_arg:
        raise ValueError('Path and filename arguments passed. Need only one of them')

    if path_arg:
        filename = path_arg
        file_type = format_arg
    elif filename_arg is not None:
        filename = filename_arg
        file_type = type_arg
    else:
        raise ValueError('Need path argument')


    if storage_arg is None:
        # get default storage
        if 'defaults' in command_config and 'default_storage' in command_config['defaults']:
            default_storage = command_config['defaults']['default_storage']
        else:
            default_storage = 'external_data'

        if default_storage not in storages:
            raise ValueError('Default storage not configured. Wrong configuration')

        storage_arg = default_storage

    if storage_arg not in storages:
        raise ValueError('Unknown storage')

    storage_path = storages[storage_arg]
    Path(storage_path).mkdir(exist_ok=True, parents=True)

    return storage_path, filename, file_type
