import click
import click_help_colors

from . import sources, destinations
from .sources import AirbyteSource


@click.group(
    cls=click_help_colors.HelpColorsGroup,
    help_headers_color='yellow',
    help_options_color='cyan'
)


def cli():
    pass


@cli.command()
def list():
    '''
    List downloadable Airbyte Source Python Connectors
    '''
    print('\n'.join(sorted(sources.list_python_airbyte_sources())))


@cli.command()
@click.argument('airbyte_connector')
@click.option('--airbyte_release', default='v0.40.30', help='defaults to `v0.40.30` (explore airbyte releases at https://github.com/airbytehq/airbyte/releases)')
def get(airbyte_connector, airbyte_release):
    '''
    Download `airbyte_connector` code from AirByte GitHub repo into local `airbyte_connectors` folder.
    `airbyte_connector` must be one of python airbyte sources returned by the commmand command `bigloader list-source-connectors`
    '''
    AirbyteSource(airbyte_connector).download(airbyte_release=airbyte_release)


@cli.command()
@click.argument('airbyte_connector')
def install(airbyte_connector):
    '''
    Install connector located at `airbyte_connectors/{airbyte_connector}` with pip
    '''
    source = AirbyteSource(airbyte_connector)
    source.install()
    source.init_config()



@cli.command()
@click.argument('airbyte_connector')
def run(airbyte_connector):
    '''
    Install `airbyte_connector` located `airbyte_connectors` folder with pip
    '''
    # AirbyteSource(airbyte_connector).init_config()
    source = AirbyteSource(airbyte_connector)
    streams = source.streams
    catalog = source.configured_catalog
    destination = destinations.LocalJsonDestination('test', streams)
    messages = source.run('read', catalog=catalog, print_log=False)
    destination.run(messages)


