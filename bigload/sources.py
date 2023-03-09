import shutil
import re
import io
import zipfile
import urllib.request
import tempfile
import os
import subprocess
import sys
import json
import platform
import pathlib
import venv
import shutil

import yaml
import click
import airbyte_cdk.models

from . import airbyte_utils


AIRBYTE_CONNECTORS_FOLDER = 'airbyte_connectors'
VIRTUAL_ENVS_FOLDER = '.venv'
PYTHON_FOLDER = {'Linux': 'bin', 'Darwin': 'bin', 'Windows': 'Scripts'}[platform.system()]




def print_color(msg):
    click.echo(click.style(msg, fg='cyan'))


def print_success(msg):
    click.echo(click.style(f'SUCCESS: {msg}', fg='green'))


def print_info(msg):
    click.echo(click.style(f'INFO: {msg}', fg='yellow'))


def print_command(msg):
    click.echo(click.style(f'RUNNING: `{msg}`', fg='magenta'))


def print_warning(msg):
    click.echo(click.style(f'WARNING: {msg}', fg='cyan'))


def handle_error(msg):
    click.echo(click.style(f'ERROR: {msg}', fg='red'))
    sys.exit()


def to_camelcase(snake_case_string):
    return ''.join(
        word.title() if k != 0 else word
        for k, word in enumerate(snake_case_string.split('_'))
    )

def create_virtual_env(virtual_env_folder):
    print_info(f'Creating virtual env at {virtual_env_folder}')
    if os.path.exists(virtual_env_folder):
        shutil.rmtree(virtual_env_folder)
    venv.create(virtual_env_folder, with_pip=True)


def download_airbyte_code_from_github(airbyte_release='master'):
    url = f'https://github.com/airbytehq/airbyte/zipball/{airbyte_release}'
    resp = urllib.request.urlopen(url)
    return zipfile.ZipFile(io.BytesIO(resp.read()))


def list_python_airbyte_sources(airbyte_release='master'):
    airbyte_archive = download_airbyte_code_from_github(airbyte_release=airbyte_release)
    pattern = r'airbyte-integrations/connectors/source-([\w-]+)/setup.py'
    return [
        'source-' + re.findall(pattern, path)[0]
        for path in airbyte_archive.namelist()
        if re.findall(pattern, path)
    ]


def check_airbyte_source_exists_and_is_a_python_connector(airbyte_source, airbyte_release='master'):
    url = f'https://api.github.com/repos/airbytehq/airbyte/contents/airbyte-integrations/connectors/{airbyte_source}/setup.py'
    try:
        urllib.request.urlopen(url)
    except:
        handle_error(f'Airbyte source `{airbyte_source}` could not be found on Airbyte GitHub repo for release {airbyte_release}. To get the full list of available airbyte sources please run `bigloader list-source-connectors`')
    return True


class AirbyteSource:

    def __init__(self, name):
        self.name = name
        self.folder = f'{AIRBYTE_CONNECTORS_FOLDER}/{name}'
        self.virtualenv_folder = f'{VIRTUAL_ENVS_FOLDER}/{name}'
        self.python_exe = str(pathlib.Path(f'{self.virtualenv_folder}/{PYTHON_FOLDER}/python'))
        self.python_command = f'{self.python_exe} {self.folder}/main.py'
        self.config_file = f'{self.folder}/bigloader_config.yaml'

    def download(self, airbyte_release='master'):
        print_info(f'Downloading airbyte GitHub repo as a zip archive')
        airbyte_archive = download_airbyte_code_from_github(airbyte_release)
        connector_path = f'airbyte-integrations/connectors/{self.name}/'
        connector_folder = next(path for path in airbyte_archive.namelist() if path.endswith(connector_path))
        connector_files = [path for path in airbyte_archive.namelist() if connector_folder in path]
        with tempfile.TemporaryDirectory() as tmpdirname:
            print_info('Extracting connector files from zip archive')
            airbyte_archive.extractall(tmpdirname, members=connector_files)
            print_info(f'Copying connector files into {self.folder}')
            if os.path.exists(self.folder):
                shutil.rmtree(self.folder)
            os.makedirs(self.folder)
            shutil.copytree(f'{tmpdirname}/{connector_folder}', self.folder, dirs_exist_ok=True)
        print_success(f'Successfully downloaded "{self.name}" airbyte connector into "{self.folder}" folder')

    def install(self):
        if os.path.exists(self.virtualenv_folder):
            print_info(f'Airbyte connector is already installed')
            print_info(f'If you wish to reinstall it, remove the folder `{self.virtualenv_folder}` and restart this command')
            return
        create_virtual_env(self.virtualenv_folder)
        print_info(f'Installing airbyte connector {self.name} located at {self.folder} via pip')
        command = f'{self.python_exe} -m pip install -e {self.folder}'
        print_command(command)
        result = os.system(command)
        if result != 0:
            handle_error('Could not install package')
        print_success(f'Successfully installed python package located at {self.folder}')

    def run(self, args, print_log=True):
        with tempfile.TemporaryDirectory() as temp_dir:
            command = f'{self.python_command} {args}'
            needs_config = 'spec' not in args
            if needs_config:
                config_filename = f'{temp_dir}/config.json'
                json.dump(self.config, open(config_filename, 'w', encoding='utf-8'))
                command += f' --config {config_filename}'
            process = subprocess.Popen(command, stdout=subprocess.PIPE)
            for line in iter(process.stdout.readline, b""):
                message = airbyte_cdk.models.AirbyteMessage.parse_raw(line)
                if (message.type == airbyte_cdk.models.Type.LOG) and print_log:
                    print_info(message.log.json(exclude_unset=True))
                elif message.type == airbyte_cdk.models.Type.TRACE:
                    handle_error(message.trace.error.message)
                else:
                    yield message

    def init_config(self):
        print_info('Generating config file')
        yaml_config = airbyte_utils.generate_connection_yaml_config_sample(self.spec)
        with open(self.config_file, 'w', encoding='utf-8') as out:
            out.write(yaml_config)
        print_success(f'Config file as been successfully written at `{self.config_file}`')
        print_warning('PLEASE make desired changes to this configuration file before running connector!')

    @property
    def config(self):
        return yaml.load(open(self.config_file, encoding='utf-8'), Loader=yaml.loader.SafeLoader)['configuration']

    @property
    def spec(self):
        messages = self.run('spec')
        message = next(messages)
        return message.spec.dict(exclude_unset=True)

    @property
    def catalog(self):
        messages = self.run('discover')
        message = next(messages)
        return message.catalog.dict(exclude_unset=True)

    @property
    def configured_catalog(self):
        catalog = self.catalog
        catalog['streams'] = [
            {
                "stream": stream,
                "sync_mode": "incremental" if 'incremental' in stream['supported_sync_modes'] else 'full_refresh',
                "destination_sync_mode": "append",
                "cursor_field": stream.get('default_cursor_field', [])
            }
            for stream in catalog['streams']
        ]
        return catalog

    @property
    def streams(self):
        return [stream['name'] for stream in self.catalog['streams']]

    def check(self):
        messages = self.run('check')
        message = next(messages)
        return message.connectionStatus.dict(exclude_unset=True)