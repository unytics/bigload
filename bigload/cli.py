import json
import os
import sys
import urllib.request


import click
from click_help_colors import HelpColorsGroup

from . import sources


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


@click.group(
    cls=HelpColorsGroup,
    help_headers_color='yellow',
    help_options_color='cyan'
)
def cli():
    pass


@cli.command()
@click.argument('source')  #, help='Airbyte Source Python Connector (to get the list of connectors run `bigload list-source-connectors`)')
@click.argument('destination')  #, default='bigquery', help='Destination among [bigquery]. Defaults to bigquery')
def install_connectors(source, destination):
    '''
    Install SOURCE and DESTINATION

        - SOURCE: one of python airbyte sources returned by command `bigload list-source-connectors`

        - DESTINATION: for now, only bigquery is accepted as DESTINATION
    '''
    if not source:
        handle_error('--source argument must be specified.\n\n' + install.__doc__)
    print_info(f'Downloading Source Airbyte Connector `{source}` from Github')
    source = sources.AirbyteSource(source)
    source.download_source_code_from_github()
    requirements = (
        source.python_requirements +
        ['google-cloud-bigquery']
    )
    print_info('Installing python requirements using:')
    command = 'pip install ' + " ".join([f'"{r}"' for r in requirements])
    print_command(command)
    result = os.system(command)
    if result != 0:
        handle_error('Could not install paclages')
    print_info('Generating Source Connector Config file')
    config_filename = source.generate_config_sample()
    print_success(f'Install run successfully. Edit generated config file `{config_filename}` before running `bigload run`')



@cli.command()
def list_source_connectors():
    '''
    List Airbyte Source Python Connectors
    '''
    print('\n'.join(sorted(sources.get_python_airbyte_source_connectors())))

