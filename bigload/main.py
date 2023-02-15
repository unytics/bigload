import os
import json
import importlib
import tempfile
import platform

import airbyte_cdk.entrypoint



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

    def read(self, streams=None, messages_handler=None):
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

            if messages_handler is None:
                messages_handler = lambda messages: [print(message) for message in messages]
            messages_handler(messages)

    def fix_connector_issues(self):
        if self._source.name == 'SourceSurveymonkey' and platform.system() == 'Windows':
            # Fix NamedTempFile problem on windows
            import source_surveymonkey.streams
            source_surveymonkey.streams.cache_file = tempfile.NamedTemporaryFile(delete=False)


if __name__ == '__main__':

    source_name = 'source-surveymonkey'

    config_filename = 'bigloads/source-surveymonkey__v0.40.30__to__bigquery.yaml'
    assert os.path.exists(config_filename), 'No config file present, generate one!'
    import yaml
    config = yaml.load(open(config_filename, encoding='utf-8'), Loader=yaml.loader.SafeLoader)
    source_config = config['configuration']

    source = AirbyteSource(source_name, source_config)
    # print(source.catalog)
    source.read()