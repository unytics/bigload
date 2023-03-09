import click
import click_help_colors

from .sources import AirbyteSource
from . import destinations


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
    source = AirbyteSource(airbyte_connector)
    streams = source.streams
    catalog = source.configured_catalog
    destination = destinations.PrintDestination('test', streams)
    messages = source.run('read', catalog=catalog, print_log=False)
    destination.run(messages)


@cli.command()
def list_source_connectors():
    '''
    List Airbyte Source Python Connectors
    '''
    print('\n'.join(sorted(_install.list_python_airbyte_sources())))

