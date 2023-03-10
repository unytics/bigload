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
@click.option('--airbyte_release', default='master', help='Example: `v0.40.30`. Defaults to `master` (explore airbyte releases at https://github.com/airbytehq/airbyte/releases)')
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
    Install local `airbyte_connector` located at `airbyte_connectors/{airbyte_connector}` with pip
    '''
    source = AirbyteSource(airbyte_connector)
    source.install()
    source.init_config()


def add_destinations_doc(function):
    destinations_doc = '\n    '.join([f'• {destination.get_init_help()}' for destination in destinations.DESTINATIONS.values()])
    function.__doc__ = function.__doc__.format(ACCEPTED_DESTINATIONS=destinations_doc)
    return function


@cli.command()
@click.argument('airbyte_connector')
@click.option('--destination', default='print()', help='extracted data destination')
@add_destinations_doc
def run(airbyte_connector, destination):
    '''
    Run `airbyte_connector` extract job

    If `--destination` is provided, extracted data will be streamed to destination, else data will be printed on console.

    \b
    Accepted `--destination` values are:
    {ACCEPTED_DESTINATIONS}

    (PLEASE replace uppercase variables such as FOLDER with values in the above destinations)
    '''
    source = AirbyteSource(airbyte_connector)
    catalog = source.configured_catalog
    destination = destinations.create_destination(destination, catalog)
    messages = source.run('read', catalog=catalog, print_log=False)
    destination.run(messages)



