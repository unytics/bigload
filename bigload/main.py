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

    def __init__(self, config, streams, buffer_size_max=1000):
        self.buffer_size_max = buffer_size_max
        import google.cloud.bigquery
        self.bigquery = google.cloud.bigquery.Client()
        self.stream_table = lambda stream: config['table'].format(stream='_' + stream) if stream else None
        self.logs_table = config['table'].format(stream='_logs')
        self.states_table = config['table'].format(stream='_states')
        print(streams)
        for stream in streams:
            self.bigquery.query(f'''
                create table if not exists {self.stream_table(stream)} (
                    job_started_at timestamp,
                    slice_started_at timestamp,
                    inserted_at timestamp,
                    data string
                )
            ''').result()
        self.bigquery.query(f'''
            create table if not exists {self.logs_table} (
                job_started_at timestamp,
                slice_started_at timestamp,
                inserted_at timestamp,
                level string,
                data string
            )
        ''').result()
        self.bigquery.query(f'''
            create table if not exists {self.states_table} (
                job_started_at timestamp,
                slice_started_at timestamp,
                inserted_at timestamp,
                state string
            )
        ''').result()
        super().__init__(config)

    def insert_rows(self, table, rows):
        if not rows:
            return
        now  = datetime.datetime.utcnow().isoformat()
        rows = [
            {
                **row,
                **{
                    'inserted_at': now,
                    'job_started_at': self.job_started_at,
                    'slice_started_at': self.slice_started_at,
                }
            }
            for row in rows
        ]
        errors = self.bigquery.insert_rows_json(table, rows)
        if errors:
            raise ValueError(f'Could not insert rows to BigQuery table {table}. Errors: {errors}')

    def handle_messages(self, messages):
        self.slice_started_at = datetime.datetime.utcnow().isoformat()
        buffer = []
        stream_table = None
        for message in messages:
            message = json.loads(message)
            if message['type'] == 'RECORD':
                new_stream_table = self.stream_table(message['record']['stream'])
                if new_stream_table != stream_table:
                    self.insert_rows(stream_table, buffer)
                    buffer = []
                    self.slice_started_at = datetime.datetime.utcnow().isoformat()
                stream_table = new_stream_table
                buffer.append({'data': json.dumps(message['record']['data'])})
                if len(buffer) > self.buffer_size_max:
                    self.insert_rows(stream_table, buffer)
                    buffer = []
            elif message['type'] == 'STATE':
                self.insert_rows(stream_table, buffer)
                buffer = []
                self.insert_rows(self.states_table, [{'state': json.dumps(message['state'])}])
                self.slice_started_at = datetime.datetime.utcnow().isoformat()
            elif message['type'] == 'LOG':
                level = logging.getLevelName(message['log']['level'])
                message = message['log']['message']
                logger.log(level, message)
            else:
                raise NotImplementedError(f'message type {message["type"]} is not managed yet')
        self.insert_rows(stream_table, buffer)

    def handle_log_message(self, level, message):
        self.insert_rows(self.logs_table, [{'level': level, 'data': message}])

    def get_state(self):
        rows = self.bigquery.query(f'''
        select json_extract(state, '$.data') as state
        from {self.states_table}
        order by inserted_at desc
        limit 1
        ''').result()
        rows = list(rows)
        return json.loads(rows[0].state) if rows else {}


def run_extract_load(source_name, source_config, destination_config, streams=None):
    source = AirbyteSource(source_name, source_config)
    destination = BigQueryDestination(destination_config, streams=streams or source.streams)
    patch_logger_to_send_logs_to_destination(destination)
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
    source_name = config_filename.replace('\\', '/').split('/')[-1].split('__')[0]
    config = yaml.load(open(config_filename, encoding='utf-8'), Loader=yaml.loader.SafeLoader)
    source_config = config['source_configuration']
    destination_config = config['destination_configuration']
    run_extract_load(source_name, source_config, destination_config, streams)

