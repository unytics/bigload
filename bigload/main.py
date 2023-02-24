import os
import json
import importlib
import tempfile
import platform
import datetime
import logging
import yaml


import airbyte_cdk.entrypoint
import airbyte_cdk.logger


logger = logging.getLogger('bigloader')
logger.setLevel('INFO')


def patch_logger_to_send_logs_to_destination(destination):

    class LogFormatter(airbyte_cdk.logger.AirbyteLogFormatter):
        def format(self, record):
            message = super().format(record)
            try:
                message = json.loads(message)['log']['message']
            except:
                pass
            try:
                destination.handle_log_message(record.levelname, message)
            except:
                print('Could not log message', message)
            return message

    root_logger = logging.getLogger()
    assert root_logger.hasHandlers(), 'root logger should have a handler'
    assert len(root_logger.handlers) == 1, 'root logger should have only one handler'
    root_logger.handlers[0].setFormatter(LogFormatter('%(message)s'))



class AirbyteSource:

    def __init__(self, name, config):
        self.name = name
        self.config = config
        self.package_name = self.name.replace("-", "_")
        self.package = importlib.import_module(self.package_name)
        self.class_name = self.name.replace('-', ' ').title().replace(' ', '')
        self.SourceClass = getattr(self.package, self.class_name)
        self.source = self.SourceClass()
        self.spec = self.source.spec(logger)
        self.entrypoint = airbyte_cdk.entrypoint.AirbyteEntrypoint(self.source)

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

    def read(self, streams=None, handle_messages=None, state=None, debug=False):
        with tempfile.TemporaryDirectory() as temp_dir:
            args = ['read']
            if debug:
                args += ['--debug']

            filename = f'{temp_dir}/config.json'
            json.dump(self.config, open(filename, 'w', encoding='utf-8'))
            args += ['--config', filename]

            catalog = self.configured_catalog
            streams = streams or [s['stream']['name'] for s in catalog['streams']]
            catalog['streams'] = [s for s in catalog['streams'] if s['stream']['name'] in streams]
            filename = f'{temp_dir}/catalog.json'
            json.dump(catalog, open(filename, 'w', encoding='utf-8'))
            args += ['--catalog', filename]

            state = state or {}
            if state:
                filename = f'{temp_dir}/state.json'
                json.dump(state, open(filename, 'w', encoding='utf-8'))
                args += ['--state', filename]

            logger.info(json.dumps({
                'message': f'starting job with source {self.name}',
                'streams': streams,
                'config': self.config,
                'catalog': catalog,
                'state': state,
            }, indent=4))

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

    def handle_messages(self, messages):
        for message in messages:
            print(message)

    def handle_log_message(self, level, message):
        pass

    def get_state(self):
        return {}


class BigQueryDestination(BaseDestination):

    def __init__(self, config, buffer_size_max=1000):
        self.table = config['table']
        self.buffer_size_max = buffer_size_max
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
            elif message['type'] == 'LOG':
                level = logging.getLevelName(message['log']['level'])
                message = message['log']['message']
                logger.log(level, message)
            else:
                raise ValueError(f'unexpected message type {message["type"]}')

            if message['type'] == 'STATE' or len(buffer) > self.buffer_size_max:
                errors = self.bigquery.insert_rows_json(self.table, buffer)
                if errors:
                    raise ValueError(f'Could not insert rows to BigQuery table. Errors: {errors}')
                buffer = []
                self.slice_started_at = datetime.datetime.utcnow().isoformat()
        if buffer:
            errors = self.bigquery.insert_rows_json(self.table, buffer)
            if errors:
                raise ValueError(f'Could not insert rows to BigQuery table. Errors: {errors}')

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

    def get_state(self):
        rows = self.bigquery.query(f'''
        select json_extract(data, '$.data') as state
        from {self.table}
        where type = 'STATE'
        order by inserted_at desc
        limit 1
        ''').result()
        rows = list(rows)
        return json.loads(rows[0].state) if rows else {}


def run_extract_load(config):
    destination_config = config['destination_configuration']
    destination = BigQueryDestination(destination_config)
    patch_logger_to_send_logs_to_destination(destination)

    source_config = config['source_configuration']
    source_name = config_filename.replace('\\', '/').split('/')[-1].split('__')[0]
    source = AirbyteSource(source_name, source_config)
    state = destination.get_state()
    source.read(handle_messages=destination.handle_messages, state=state, streams=streams)


if __name__ == '__main__':

    import argparse
    parser = argparse.ArgumentParser(
        prog='bigloader job runner',
        description='Run extract-load job Run Extract-Load job from `airbyte_source` (of `airbyte_release`) to `destination` as defined in `config_filename`',
    )
    parser.add_argument('config_filename')
    parser.add_argument('--streams')
    args = parser.parse_args()
    config_filename = args.config_filename
    streams = args.streams.split(',') if args.streams else None
    assert os.path.exists(config_filename), f'Config file {config_filename} does not exist. Please create one!'
    config = yaml.load(open(config_filename, encoding='utf-8'), Loader=yaml.loader.SafeLoader)
    run_extract_load(config)

