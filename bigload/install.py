import re
import io
import zipfile
import urllib.request
import tempfile
import os
import subprocess
import sys
import json

import click

from . import airbyte_utils


AIRBYTE_ARCHIVE = None


def print_color(msg):
    click.echo(click.style(msg, fg='cyan'))

def print_success(msg):
    click.echo(click.style(f'SUCCESS: {msg}', fg='green'))

def print_info(msg):
    click.echo(click.style(f'INFO: {msg}', fg='yellow'))

def print_command(msg):
    click.echo(click.style(f'INFO: `{msg}`', fg='magenta'))

def print_warning(msg):
    click.echo(click.style(f'WARNING: {msg}', fg='cyan'))

def handle_error(msg):
    click.echo(click.style(f'ERROR: {msg}', fg='red'))
    sys.exit()


def download_airbyte_code_from_github(branch_or_release='master'):
    global AIRBYTE_ARCHIVE
    if AIRBYTE_ARCHIVE is None:
        url = f'https://github.com/airbytehq/airbyte/zipball/{branch_or_release}'
        resp = urllib.request.urlopen(url)
        AIRBYTE_ARCHIVE = zipfile.ZipFile(io.BytesIO(resp.read()))
    return AIRBYTE_ARCHIVE


def list_python_airbyte_sources(branch_or_release='master'):
    airbyte_archive = download_airbyte_code_from_github(branch_or_release=branch_or_release)
    pattern = r'airbyte-integrations/connectors/source-([\w-]+)/setup.py'
    return [
        'source-' + re.findall(pattern, path)[0]
        for path in airbyte_archive.namelist()
        if re.findall(pattern, path)
    ]


def install_airbyte_source(airbyte_source, branch_or_release='master', python_exe=None):
    print_info(f'Downloading airbyte_source code from GitHub')
    airbyte_archive = download_airbyte_code_from_github(branch_or_release)
    connector_path = f'airbyte-integrations/connectors/{airbyte_source}/'
    connector_folder = next(path for path in airbyte_archive.namelist() if path.endswith(connector_path))
    connector_files = [path for path in airbyte_archive.namelist() if connector_folder in path]
    with tempfile.TemporaryDirectory() as tmpdirname:
        airbyte_archive.extractall(tmpdirname, members=connector_files)
        python_exe = python_exe or 'python'
        command = f'{python_exe} -m pip install {tmpdirname}/{connector_folder}'
        print_info(f'Installing airbyte_source via pip')
        print_command(command)
        result = os.system(command)
        if result != 0:
            handle_error('Could not install airbyte_source')


def install_destination(destination, python_exe=None):
    python_exe = python_exe or 'python'
    if destination == 'bigquery':
        command = f'{python_exe} -m pip install "google-cloud-bigquery"'
    else:
        handle_error(f'destination {destination} is not yet supported. We only support BigQuery for now')
    print_info(f'Installing destination via pip')
    print_command(command)
    result = os.system(command)
    if result != 0:
        handle_error('Could not install airbyte_source')


def install(airbyte_source, airbyte_release, destination, python_exe=None):
    install_airbyte_source(airbyte_source, branch_or_release=airbyte_release, python_exe=python_exe)
    install_destination(destination, python_exe=python_exe)


def generate_airbyte_source_config_sample(airbyte_source, python_exe=None):
    python_exe = python_exe or 'python'

    package_name = airbyte_source.replace("-", "_")
    class_name = airbyte_source.replace('-', ' ').title().replace(' ', '')
    python_command = '; '.join([
        f'import {package_name}',
        f'import logging',
        f'print({package_name}.{class_name}().spec(logging.getLogger()).json())',
    ])
    spec = subprocess.check_output([python_exe, '-c', python_command])
    spec = json.loads(spec.decode())
    print(spec)

    return airbyte_utils.generate_connection_yaml_config_sample(spec)
