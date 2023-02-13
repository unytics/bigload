import re
import io
import os
import shutil
import zipfile
import json
import sys
import importlib
import tempfile
import distutils
import urllib.request
import platform


AIRBYTE_ARCHIVE = None


def download_airbyte_code_from_github(branch_or_release='master'):
    global AIRBYTE_ARCHIVE
    if AIRBYTE_ARCHIVE is None:
        url = f'https://github.com/airbytehq/airbyte/zipball/{branch_or_release}'
        resp = urllib.request.urlopen(url)
        AIRBYTE_ARCHIVE = zipfile.ZipFile(io.BytesIO(resp.read()))
    return AIRBYTE_ARCHIVE


def get_python_airbyte_source_connectors(branch_or_release='master'):
    airbyte_archive = download_airbyte_code_from_github(branch_or_release=branch_or_release)
    pattern = r'airbyte-integrations/connectors/source-([\w-]+)/setup.py'
    return [
        'source-' + re.findall(pattern, path)[0]
        for path in airbyte_archive.namelist()
        if re.findall(pattern, path)
    ]


class AirbyteSource:

    base_code_folder = 'airbyte_connectors'
    base_config_folder = 'airbyte_connectors_config'

    def __init__(self, name):
        self.name = name
        self._source = None
        self._entrypoint = None

    @property
    def code_folder(self):
        return f'{self.base_code_folder}/{self.name}'

    @property
    def config_filename(self):
        return f'{self.base_config_folder}/{self.name}.yaml'

    @property
    def source(self):
        if self._source is None:
            sys.path.insert(1, self.code_folder)
            package_name = self.name.replace("-", "_")
            package = importlib.import_module(package_name)
            class_name = self.name.replace('-', ' ').title().replace(' ', '')
            Source = getattr(package, class_name)
            self._source = Source()
        return self._source

    @property
    def connection_spec(self):
        return self.spec['connectionSpecification']

    @property
    def entrypoint(self):
        if self._entrypoint is None:
            import airbyte_cdk.entrypoint
            self._entrypoint = airbyte_cdk.entrypoint.AirbyteEntrypoint(self.source)
        return self._entrypoint

    @property
    def config(self):
        assert os.path.exists(self.config_filename), 'No config file present, generate one!'
        import yaml
        config = yaml.load(open(self.config_filename, encoding='utf-8'), Loader=yaml.loader.SafeLoader)
        return config['configuration']

    def generate_config_sample(self):
        from . import airbyte_utils
        with open(self.config_filename, 'w') as out:
            config = airbyte_utils.generate_connection_yaml_config_sample(self.spec)
            out.write(config)
        return self.config_filename

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

    def download_source_code_from_github(self, branch_or_release='master', force=False):
        airbyte_archive = download_airbyte_code_from_github(branch_or_release)
        connector_path = f'airbyte-integrations/connectors/{self.name}/'
        connector_folder = next(path for path in airbyte_archive.namelist() if path.endswith(connector_path))
        connector_files = [path for path in airbyte_archive.namelist() if connector_folder in path]
        with tempfile.TemporaryDirectory() as tmpdirname:
            airbyte_archive.extractall(tmpdirname, members=connector_files)
            if os.path.exists(self.code_folder):
                shutil.rmtree(self.code_folder)
            shutil.move(f'{tmpdirname}/{connector_folder}', self.code_folder)

    @property
    def python_requirements(self):
        py_setup_file = f'{self.code_folder}/setup.py'
        setup = distutils.core.run_setup(py_setup_file, stop_after='init')
        return setup.install_requires



# source_name = 'source-surveymonkey'

# source = AirbyteSource(source_name)
# source.generate_config_sample()

# print(get_python_airbyte_source_connectors())


# print(source.catalog)

# source.download_source_code_from_github()
# source.install()

# # print(source.spec)
# print(source.sample_config)
# source.read()