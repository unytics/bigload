import sys

import click


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
