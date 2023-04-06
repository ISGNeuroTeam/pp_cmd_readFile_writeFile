# pp_cmd_readFile_writeFile
Postprocessing commands "readFile" and "writeFile"

## Description
`readFile` reads file from storage  
`writeFile` writes file to storage  
Storages configured in `config.ini` in `storages` section  


### Arguments
- filename - first positional argument. Relative filename in storage. May include path, example: "path1/path2/file_name.csv"
- type - keyword argument, file type. Supported types: csv, json, parquet
- storage - keyword argument, storage to save(read), default is `lookups`.
- private - keyword argument, save to (read from) user directory in storage. 
- mode - keyword argument, write mode for writeFile command, default is `overwrite`.  
    Possible values are:  
    * overwrite - overwrite file in storage  
    * append - append dataframe to file  
    * ignore - ignores write operation when the file already exists
- path - keyword argument, not required, the same as `filename` (for alternative syntax)
- format - keyword argument, not required, the same as `type` (for alternative syntax)
  
### Usage example
`... | readFile books.csv, type=csv`
#### Using paths in storage
`... | readFile "some_folder_in_storage/books.csv, type=csv"`
#### Using another storage
`... | readFile "some_folder_in_storage/books.csv, type=csv, storage=pp_shared"`
#### Using private user folder
`... | readFile "some_folder_in_storage/books.csv, type=csv, storage=pp_shared", private=true`  
In that case absolute path to file is `<storage_path>/<user_guid>/<file_path_in_storage>`
#### Using append mode in `writeFile`
`... | writeFile books.csv, mode=append`

#### Using alternative syntax with `path` and `format` keywords  
`... | readFile path="books.csv", format="csv"`


### Important
**Make sure that in append mode dataframe has the same columns as a target file, otherwise result file will be corrupted or exception wil be raised**

## Getting started
### Installing
1. Create virtual environment with post-processing sdk 
```bash
    make dev
```
That command  
- downloads [Miniconda](https://docs.conda.io/en/latest/miniconda.html)
- creates python virtual environment with [postprocessing_sdk](https://github.com/ISGNeuroTeam/postprocessing_sdk)
- creates link to current command in postprocessing `pp_cmd` directory 

2. Configure `otl_v1` command. Example:  
```bash
    vi ./venv/lib/python3.9/site-packages/postprocessing_sdk/pp_cmd/otl_v1/config.ini
```
Config example:  
```ini
[spark]
base_address = http://localhost
username = admin
password = 12345678

[caching]
# 24 hours in seconds
login_cache_ttl = 86400
# Command syntax defaults
default_request_cache_ttl = 100
default_job_timeout = 100
```

3. Configure storages for `readFile` and `writeFile` commands:  
```bash
   vi ./venv/lib/python3.9/site-packages/postprocessing_sdk/pp_cmd/readFile/config.ini
   
```
Config example:  
```ini
[storages]
lookups = /opt/otp/lookups
external_data = /opt/otp/external_data
pp_shared = /opt/otp/shared_storage/persistent

[defaults]
default_storage = external_data
```

### Run readFile_writeFile
Use `pp` to run readFile_writeFile command:  
```bash
pp
Storage directory is /tmp/pp_cmd_test/storage
Commmands directory is /tmp/pp_cmd_test/pp_cmd
query: | otl_v1 <# makeresults count=100 #> |  writeFile test.csv
```
## Deploy
1. Unpack archive `pp_cmd_readFile_writeFile` to postprocessing commands directory
2. Configure `config.ini` with path to storages (default is `loolups` in `/opt/otp/lookups`)
```
cp /opt/otp/python_computing_node/commands/readFile/config.example.ini /opt/otp/python_computing_node/commands/readFile/config.ini 
```