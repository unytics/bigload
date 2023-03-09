import os



import click
import click_help_colors

from . import source
from .source import AirbyteSource


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
    AirbyteSource(airbyte_connector).download(airbyte_release=airbyte_release)


@cli.command()
@click.argument('airbyte_connector')
def install_source_connector(airbyte_connector):
    '''
    Install `airbyte_connector` located *airbyte_connectors* folder with pip
    '''
    AirbyteSource(airbyte_connector).install()



@cli.command()
@click.argument('airbyte_connector')
def spec(airbyte_connector):
    '''
    Install `airbyte_connector` located *airbyte_connectors* folder with pip
    '''
    # AirbyteSource(airbyte_connector).init_config()
    print(AirbyteSource(airbyte_connector).check())




# @cli.command()
# @click.argument('airbyte_source')
# @click.option('--destination', default='bigquery', help='defaults to `bigquery` (only bigquery is supported for now)')
# @click.option('--streams', help='comma separated list of streams to extract')
# def run(airbyte_source, destination, streams):
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
#     conf = Configuration(airbyte_source, destination, streams)

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
def list_source_connectors():
    '''
    List Airbyte Source Python Connectors
    '''
    print('\n'.join(sorted(_install.list_python_airbyte_sources())))

