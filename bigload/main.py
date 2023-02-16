import os
import json
import importlib
import tempfile
import platform
import datetime

import airbyte_cdk.entrypoint
import airbyte_cdk.logger


class AirbyteSource:

    def __init__(self, name, config):
        self.name = name
        self._source = None
        self._entrypoint = None
        self.config = config

    @property
    def source(self):
        if self._source is None:
            package_name = self.name.replace("-", "_")
            package = importlib.import_module(package_name)
            class_name = self.name.replace('-', ' ').title().replace(' ', '')
            Source = getattr(package, class_name)
            self._source = Source()
        return self._source

    @property
    def entrypoint(self):
        if self._entrypoint is None:
            self._entrypoint = airbyte_cdk.entrypoint.AirbyteEntrypoint(self.source)
        return self._entrypoint

    @property
    def spec(self):
        return self.run('spec')

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
        if self._source.name == 'SourceSurveymonkey' and platform.system() == 'Windows':
            # Fix NamedTempFile problem on windows
            import source_surveymonkey.streams
            source_surveymonkey.streams.cache_file = tempfile.NamedTemporaryFile(delete=False)


class BaseDestination:

    def __init__(self, config):
        https://stackoverflow.com/questions/6847862/how-to-change-the-format-of-logged-messages-temporarily-in-python
        _log_formatter = airbyte_cdk.logger.AirbyteLogFormatter.format
        def new_log_formatter(_self, record):
            formatted_log = _log_formatter(_self, record)
            self.handle_log_message(formatted_log)
            return formatted_log
        airbyte_cdk.logger.AirbyteLogFormatter.format = new_log_formatter


class BigQueryDestination(BaseDestination):

    def __init__(self, config):
        job_id = uuid
        start_date =
        self.table = config['table']
        import google.cloud.bigquery
        self.bigquery = google.cloud.bigquery.Client()
        self.bigquery.query(f'''
            create table if not exists {self.table} (
                timestamp timestamp,
                type string,
                stream string,
                data string
            )
        ''').result()
        super().__init__(config)


    def handle_messages(self, messages):
        buffer = []
        state_id = uuid
        for message in messages:
            message = json.loads(message)
            if message['type'] == 'RECORD':
                message = {
                    'timestamp': datetime.datetime.utcnow().isoformat(),
                    'type': message['type'],
                    'data': json.dumps(message['record']['data']),
                    'stream': message['record']['stream']
                }
                buffer.append(message)
            elif message['type'] == 'STATE':
                message = {
                    'timestamp': datetime.datetime.utcnow().isoformat(),
                    'type': message['type'],
                    'data': json.dumps(message['state']),
                }
                buffer.append(message)
                errors = self.bigquery.insert_rows_json(self.table, buffer)
                if errors:
                    raise ValueError(f'Could not insert rows to BigQuery table. Errors: {errors}')
                buffer = []
            else:
                raise ValueError(f'unexpected message type {message["type"]}')

    def handle_log_message(self, message):
        message = json.loads(message)
        assert message['type'] == 'LOG', f'expecting LOG message type but got {message["type"]}'
        message = {
            'timestamp': datetime.datetime.utcnow().isoformat(),
            'type': message['type'],
            'data': json.dumps(message['log']),
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
    source = AirbyteSource(source_name, source_config)
    # print(source.catalog)
    source.read(handle_messages=destination.handle_messages)