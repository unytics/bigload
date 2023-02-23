import os
import json
import importlib
import tempfile
import platform
import datetime
import logging
import traceback
import sys
import uuid


import airbyte_cdk.entrypoint
import airbyte_cdk.utils.airbyte_secrets_utils


def get_logger(log_message, secrets):
    '''
    Returns a logger which:
    - remove any secret from message
    - call `log_message` function for each log message
    '''
    class LogFormatter(logging.Formatter):
        def format(_self, record):
            message = super().format(record)
            for secret in secrets:
                if secret:
                    message = message.replace(str(secret), "****")
            log_message(record.levelname, message)
            return message

    logger = logging.getLogger(str(uuid.uuid4()))
    logger_handler = logging.StreamHandler()
    logger.addHandler(logger_handler)
    logger.propagate = False
    logger_handler.setFormatter(LogFormatter('%(message)s'))
    return logger



class AirbyteSource:

    def __init__(self, name, config, handle_log_message=None):
        self.name = name
        self.config = config
        self.package_name = self.name.replace("-", "_")
        self.package = importlib.import_module(self.package_name)
        self.class_name = self.name.replace('-', ' ').title().replace(' ', '')
        self.SourceClass = getattr(self.package, self.class_name)
        self.source = self.SourceClass()
        self.spec = self.source.spec(logging.getLogger())
        self.entrypoint = airbyte_cdk.entrypoint.AirbyteEntrypoint(self.source)
        if handle_log_message is not None:
            config_secrets = airbyte_cdk.utils.airbyte_secrets_utils.get_secrets(self.spec.connectionSpecification, config)
            self.entrypoint.logger = get_logger(handle_log_message, config_secrets)

    @property
    def catalog(self):
        return self.run('discover')

    @property
    def configured_catalog(self):
        catalog = self.catalog
        catalog['streams'] = [
            {
                "stream": stream,
                "sync_mode": "incremental" if 'incremental' in stream['supported_sync_modes'] else 'full_refresh',
                "destination_sync_mode": "append"
            }
            for stream in catalog['streams']
        ]
        return catalog

    @property
    def streams(self):
        return [stream['name'] for stream in self.catalog['streams']]

    def check(self):
        return self.run('check')

    def run(self, command):
        with tempfile.TemporaryDirectory() as temp_dir:
            args = [command]
            if command != 'spec':
                config_filename = f'{temp_dir}/config.json'
                json.dump(self.config, open(config_filename, 'w', encoding='utf-8'))
                args += ['--config', config_filename]
            parsed_args = self.entrypoint.parse_args(args)
            messages = [message for message in self.entrypoint.run(parsed_args)]
            assert len(messages) == 1, f'"{command}" command should return only one message'
            message = json.loads(messages[0])
            assert len(message.keys()) == 2, 'message should have only two keys, `type` and a second one'
            value = [v for k, v in message.items() if k != 'type'][0]
            return value

    def read(self, streams=None, handle_messages=None):
        with tempfile.TemporaryDirectory() as temp_dir:
            args = ['read']

            config_filename = f'{temp_dir}/config.json'
            json.dump(self.config, open(config_filename, 'w', encoding='utf-8'))
            args += ['--config', config_filename]

            catalog = self.configured_catalog
            if streams is not None:
                catalog['streams'] = [s for s in catalog['streams'] if s['stream']['name'] in streams]
            catalog_filename = f'{temp_dir}/catalog.json'
            json.dump(catalog, open(catalog_filename, 'w', encoding='utf-8'))
            args += ['--catalog', catalog_filename]

            self.fix_connector_issues()
            parsed_args = self.entrypoint.parse_args(args)
            messages = self.entrypoint.run(parsed_args)
            handle_messages(messages)

    def fix_connector_issues(self):
        if self.source.name == 'SourceSurveymonkey' and platform.system() == 'Windows':
            # Fix NamedTempFile problem on windows
            import source_surveymonkey.streams
            source_surveymonkey.streams.cache_file = tempfile.NamedTemporaryFile(delete=False)


class BaseDestination:

    def __init__(self, config):
        self.config = config
        self.job_started_at = datetime.datetime.utcnow().isoformat()
        self.slice_started_at = None

    def handle_log_message(self, level, message):
        pass

class BigQueryDestination(BaseDestination):

    def __init__(self, config):
        self.table = config['table']
        import google.cloud.bigquery
        self.bigquery = google.cloud.bigquery.Client()
        self.bigquery.query(f'''
            create table if not exists {self.table} (
                job_started_at timestamp,
                slice_started_at timestamp,
                inserted_at timestamp,
                type string,
                subtype string,
                stream string,
                data string
            )
        ''').result()
        super().__init__(config)

    def handle_messages(self, messages):
        self.slice_started_at = datetime.datetime.utcnow().isoformat()
        buffer = []
        for message in messages:
            message = json.loads(message)
            if message['type'] == 'RECORD':
                message = {
                    'inserted_at': datetime.datetime.utcnow().isoformat(),
                    'job_started_at': self.job_started_at,
                    'slice_started_at': self.slice_started_at,
                    'type': message['type'],
                    'subtype': None,
                    'data': json.dumps(message['record']['data']),
                    'stream': message['record']['stream']
                }
                buffer.append(message)
            elif message['type'] == 'STATE':
                message = {
                    'inserted_at': datetime.datetime.utcnow().isoformat(),
                    'job_started_at': self.job_started_at,
                    'slice_started_at': self.slice_started_at,
                    'type': message['type'],
                    'subtype': message['state']['type'],
                    'data': json.dumps(message['state']),
                }
                buffer.append(message)
                errors = self.bigquery.insert_rows_json(self.table, buffer)
                if errors:
                    raise ValueError(f'Could not insert rows to BigQuery table. Errors: {errors}')
                buffer = []
                self.slice_started_at = datetime.datetime.utcnow().isoformat()
            else:
                raise ValueError(f'unexpected message type {message["type"]}')

    def handle_log_message(self, level, message):
        message = {
            'inserted_at': datetime.datetime.utcnow().isoformat(),
            'job_started_at': self.job_started_at,
            'slice_started_at': self.slice_started_at,
            'type': 'LOG',
            'subtype': level,
            'data': message,
        }
        errors = self.bigquery.insert_rows_json(self.table, [message])
        if errors:
            raise ValueError(f'Could not insert rows to BigQuery table. Errors: {errors}')



if __name__ == '__main__':

    import argparse
    parser = argparse.ArgumentParser(
        prog='bigloader job runner',
        description='Run extract-load job Run Extract-Load job from `airbyte_source` (of `airbyte_release`) to `destination` as defined in `config_filename`',
    )
    parser.add_argument('config_filename')
    args = parser.parse_args()
    config_filename = args.config_filename

    assert os.path.exists(config_filename), f'Config file {config_file} does not exist. Please create one!'

    import yaml
    config = yaml.load(open(config_filename, encoding='utf-8'), Loader=yaml.loader.SafeLoader)


    destination_config = config['destination_configuration']
    destination = BigQueryDestination(destination_config)

    source_config = config['source_configuration']
    source_name = config_filename.replace('\\', '/').split('/')[-1].split('__')[0]
    source = AirbyteSource(source_name, source_config, handle_log_message=destination.handle_log_message)
    try:
        source.read(handle_messages=destination.handle_messages)
    except Exception:
        source.entrypoint.logger.error(traceback.format_exc())
        sys.exit(1)
