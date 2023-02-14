import os
import venv
import shutil
import platform
import pathlib

import click
import click_help_colors

from . import install as _install


class Configuration:

    def __init__(self, airbyte_source, airbyte_release, destination):
        self.airbyte_source = airbyte_source
        self.airbyte_release = airbyte_release
        self.destination = destination
        self.name = f'{airbyte_source}__{airbyte_release}__to__{destination}'
        self.virtual_env_folder = f'.venv/{self.name}'
        self.python_folder = {'Linux': 'bin', 'Darwin': 'bin', 'Windows': 'Scripts'}[platform.system()]
        self.python_exe = str(pathlib.Path(f'{self.virtual_env_folder}/{self.python_folder}/python'))


@click.group(
    cls=click_help_colors.HelpColorsGroup,
    help_headers_color='yellow',
    help_options_color='cyan'
)


def cli():
    pass


@cli.command()
@click.argument('airbyte_source')
@click.option('--airbyte_release', default='v0.40.30', help='defaults to `v0.40.30` (explore airbyte releases at https://github.com/airbytehq/airbyte/releases)')
@click.option('--destination', default='bigquery', help='defaults to `bigquery` (only bigquery is supported for now)')
def install(airbyte_source, airbyte_release, destination):
    '''
    Install source and destination connectors in a new python virtual environment

    > AIRBYTE_SOURCE: one of python airbyte sources returned by command `bigload list-source-connectors`

    > --destination: for now, only bigquery is supported
    '''
    conf = Configuration(airbyte_source, airbyte_release, destination)

    _install.print_info(f'Creating virtual env at {conf.virtual_env_folder}')
    if os.path.exists(conf.virtual_env_folder):
        shutil.rmtree(conf.virtual_env_folder)
    venv.create(conf.virtual_env_folder, with_pip=True)

    _install.install(airbyte_source, airbyte_release, destination, python_exe=conf.python_exe)

    _install.generate_airbyte_source_config_sample(airbyte_source, python_exe=conf.python_exe)



# @cli.command()
# @click.argument('airbyte_source')
# @click.option('--airbyte_release', default='v0.40.30', help='defaults to `v0.40.30` (explore airbyte releases at https://github.com/airbytehq/airbyte/releases)')
# @click.option('--destination', default='bigquery', help='defaults to `bigquery` (only bigquery is supported for now)')
# def run(airbyte_source, airbyte_release, destination):
#     virtualenv_folder = f'.venv/{airbyte_source}_{airbyte_release}__to__{destination}'
#     python_folder = {'Linux': 'bin', 'Darwin': 'bin', 'Windows': 'Scripts'}[platform.system()]
#     python_exe = str(pathlib.Path(f'{virtualenv_folder}/{python_folder}/python'))

#     script = __file__.replace('cli.py', '') + 'bla.py'
#     print_info('Generating Source Connector Config file')
#     command = f'{python_exe} {script} spec'
#     os.system(command)


@cli.command()
def list_source_connectors():
    '''
    List Airbyte Source Python Connectors
    '''
    print('\n'.join(sorted(install.list_python_airbyte_sources())))

