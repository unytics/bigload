import os
import venv
import shutil
import platform
import pathlib

import click
import click_help_colors

from . import install as _install

AIRBYTE_CONNECTORS_FOLDER = 'airbyte_connectors'
CONFIGURATION_FOLDER = 'extract_load_jobs'


def get_virtualenv_paths(name):
    virtual_env_folder = f'.venv/{name}'
    python_folder = {'Linux': 'bin', 'Darwin': 'bin', 'Windows': 'Scripts'}[platform.system()]
    python_exe = str(pathlib.Path(f'{virtual_env_folder}/{python_folder}/python'))
    return virtual_env_folder, python_exe

class Configuration:

    def __init__(self, airbyte_source, destination, streams):
        self.airbyte_source = airbyte_source
        self.destination = destination
        self.name = f'{airbyte_source}__to__{destination}'
        self.virtual_env_folder, self.python_exe = get_virtualenv_paths(airbyte_source)
        self.config_folder = CONFIGURATION_FOLDER
        self.config_filename = f'{self.config_folder}/{self.name}.yaml'
        self.run_job_command = ' '.join([
            self.python_exe,
            __file__.replace('cli.py', 'main.py'),
            self.config_filename,
            f'--streams {streams}' if streams else ''
        ])



def create_virtual_env(virtual_env_folder):
    _install.print_info(f'Creating virtual env at {virtual_env_folder}')
    if os.path.exists(virtual_env_folder):
        shutil.rmtree(virtual_env_folder)
    venv.create(virtual_env_folder, with_pip=True)


@click.group(
    cls=click_help_colors.HelpColorsGroup,
    help_headers_color='yellow',
    help_options_color='cyan'
)


def cli():
    pass


@cli.command()
@click.argument('airbyte_connector')
@click.option('--airbyte_release', default='v0.40.30', help='defaults to `v0.40.30` (explore airbyte releases at https://github.com/airbytehq/airbyte/releases)')
def download_source_connector(airbyte_connector, airbyte_release):
    '''
    Download `airbyte_connector` into *airbyte_connectors* folder
    `airbyte_connector` must be one of python airbyte sources returned by the commmand command `bigloader list-source-connectors`
    '''
    _install.download_airbyte_connector(airbyte_connector, f'{AIRBYTE_CONNECTORS_FOLDER}/{airbyte_connector}', airbyte_release)


@cli.command()
@click.argument('airbyte_connector')
def install_source_connector(airbyte_connector):
    '''
    Install `airbyte_connector` located *airbyte_connectors* folder with pip
    '''
    virtual_env_folder, python_exe = get_virtualenv_paths(airbyte_connector)
    if os.path.exists(virtual_env_folder):
        _install.print_info(f'Using existing virtual env')
        _install.print_info(f'If you wish to reinstall it, remove the folder `{virtual_env_folder}` and restart this command')
    else:
        create_virtual_env(virtual_env_folder)
        _install.install_python_package(f'{AIRBYTE_CONNECTORS_FOLDER}/{airbyte_connector}', python_exe=python_exe)


@cli.command()
@click.argument('airbyte_source')
@click.option('--destination', default='bigquery', help='defaults to `bigquery` (only bigquery is supported for now)')
@click.option('--streams', help='comma separated list of streams to extract')
def run(airbyte_source, destination, streams):
    '''
    Run Extract-Load job from `airbyte_source` (of `airbyte_release`) to `destination`

    This command will:

    1. Create a virtual environment where `airbyte_source` and `destination` python packages will be installed.

    2. Generate an extract-load config file sample that you will then update with desired configuration

    3. Launch the extract-load job in the virtual environement using the config file.


    --

    Notes:

    - Steps 1 and 2 are done only once (except if you delete the virtual env folder or the config file)

    - `airbyte_source` must be one of python airbyte sources returned by the commmand command `bigloader list-source-connectors`

    - `destination` must be one of [print, bigquery]. If you desire another destination, please file a GitHub issue.
    '''
    conf = Configuration(airbyte_source, destination, streams)

    if os.path.exists(conf.config_filename):
        _install.print_info(f'Using existing config file')
        _install.print_info(f'if you wish to recreate it, remove the file `{conf.config_filename}` and restart this command')
    else:
        config = _install.generate_airbyte_source_config_sample(airbyte_source, python_exe=conf.python_exe)
        os.makedirs(conf.config_folder, exist_ok=True)
        with open(conf.config_filename, 'w', encoding='utf-8') as out:
            out.write(config)
        _install.print_success(f'Config file as been successfully written at `{conf.config_filename}`')
        _install.print_info('PLEASE make desired changes to this file before continuing')
        click.confirm('Done? Do you want to continue?', abort=True)

    _install.print_info('Starting extract-load job')
    _install.print_command(conf.run_job_command)
    result = os.system(conf.run_job_command)
    if result != 0:
        _install.handle_error('Could not successfully run extract-load job. Check logs above for details. It may be a configuration issue in your config file.')
    _install.print_success('All Good!')

# @cli.command()
# @click.argument('airbyte_source')
# @click.option('--airbyte_release', default='v0.40.30', help='defaults to `v0.40.30` (explore airbyte releases at https://github.com/airbytehq/airbyte/releases)')
# @click.option('--destination', default='bigquery', help='defaults to `bigquery` (only bigquery is supported for now)')
# @click.option('--streams', help='comma separated list of streams to extract')
# def run(airbyte_source, airbyte_release, destination, streams):
#     '''
#     Run Extract-Load job from `airbyte_source` (of `airbyte_release`) to `destination`

#     This command will:

#     1. Create a virtual environment where `airbyte_source` and `destination` python packages will be installed.

#     2. Generate an extract-load config file sample that you will then update with desired configuration

#     3. Launch the extract-load job in the virtual environement using the config file.


#     --

#     Notes:

#     - Steps 1 and 2 are done only once (except if you delete the virtual env folder or the config file)

#     - `airbyte_source` must be one of python airbyte sources returned by the commmand command `bigloader list-source-connectors`

#     - `destination` must be one of [print, bigquery]. If you desire another destination, please file a GitHub issue.
#     '''
#     conf = Configuration(airbyte_source, airbyte_release, destination, streams)

#     if os.path.exists(conf.virtual_env_folder):
#         _install.print_info(f'Using existing virtual env')
#         _install.print_info(f'If you wish to reinstall it, remove the folder `{conf.virtual_env_folder}` and restart this command')
#     else:
#         _install.check_airbyte_source_exists_and_is_a_python_connector(airbyte_source, branch_or_release=airbyte_release)
#         create_virtual_env(conf.virtual_env_folder)
#         _install.install(airbyte_source, airbyte_release, destination, python_exe=conf.python_exe)

#     if os.path.exists(conf.config_filename):
#         _install.print_info(f'Using existing config file')
#         _install.print_info(f'if you wish to recreate it, remove the file `{conf.config_filename}` and restart this command')
#     else:
#         config = _install.generate_airbyte_source_config_sample(airbyte_source, python_exe=conf.python_exe)
#         os.makedirs(conf.config_folder, exist_ok=True)
#         with open(conf.config_filename, 'w', encoding='utf-8') as out:
#             out.write(config)
#         _install.print_success(f'Config file as been successfully written at `{conf.config_filename}`')
#         _install.print_info('PLEASE make desired changes to this file before continuing')
#         click.confirm('Done? Do you want to continue?', abort=True)

#     _install.print_info('Starting extract-load job')
#     _install.print_command(conf.run_job_command)
#     result = os.system(conf.run_job_command)
#     if result != 0:
#         _install.handle_error('Could not successfully run extract-load job. Check logs above for details. It may be a configuration issue in your config file.')
#     _install.print_success('All Good!')


@cli.command()
@click.argument('airbyte_source')
@click.option('--airbyte_release', default='v0.40.30', help='defaults to `v0.40.30` (explore airbyte releases at https://github.com/airbytehq/airbyte/releases)')
@click.option('--destination', default='bigquery', help='defaults to `bigquery` (only bigquery is supported for now)')
@click.option('--streams', help='comma separated list of streams to extract')
def deploy(airbyte_source, airbyte_release, destination, streams):
    '''
    Run Extract-Load job from `airbyte_source` (of `airbyte_release`) to `destination`

    This command will:

    1. Create a virtual environment where `airbyte_source` and `destination` python packages will be installed.

    2. Generate an extract-load config file sample that you will then update with desired configuration

    3. Launch the extract-load job in the virtual environement using the config file.


    --

    Notes:

    - Steps 1 and 2 are done only once (except if you delete the virtual env folder or the config file)

    - `airbyte_source` must be one of python airbyte sources returned by the commmand command `bigloader list-source-connectors`

    - `destination` must be one of [print, bigquery]. If you desire another destination, please file a GitHub issue.
    '''
    conf = Configuration(airbyte_source, airbyte_release, destination, streams)

    if os.path.exists(conf.virtual_env_folder):
        _install.print_info(f'Using existing virtual env')
        _install.print_info(f'If you wish to reinstall it, remove the folder `{conf.virtual_env_folder}` and restart this command')
    else:
        _install.check_airbyte_source_exists_and_is_a_python_connector(airbyte_source, branch_or_release=airbyte_release)
        create_virtual_env(conf.virtual_env_folder)
        _install.install(airbyte_source, airbyte_release, destination, python_exe=conf.python_exe)

    if os.path.exists(conf.config_filename):
        _install.print_info(f'Using existing config file')
        _install.print_info(f'if you wish to recreate it, remove the file `{conf.config_filename}` and restart this command')
    else:
        config = _install.generate_airbyte_source_config_sample(airbyte_source, python_exe=conf.python_exe)
        os.makedirs(conf.config_folder, exist_ok=True)
        with open(conf.config_filename, 'w', encoding='utf-8') as out:
            out.write(config)
        _install.print_success(f'Config file as been successfully written at `{conf.config_filename}`')
        _install.print_info('PLEASE make desired changes to this file before continuing')
        click.confirm('Done? Do you want to continue?', abort=True)

    _install.print_info('Starting extract-load job')
    _install.print_command(conf.run_job_command)
    result = os.system(conf.run_job_command)
    if result != 0:
        _install.handle_error('Could not successfully run extract-load job. Check logs above for details. It may be a configuration issue in your config file.')
    _install.print_success('All Good!')

@cli.command()
def list_source_connectors():
    '''
    List Airbyte Source Python Connectors
    '''
    print('\n'.join(sorted(_install.list_python_airbyte_sources())))

