import os
import json

import airbyte_cdk


def create_file_or_try_to_open(filename):
    try:
        open(filename, 'a').close()
    except:
        raise ValueError(f'Cannot create or open file {filename}')


def get_latest_line_of_file(filename):
    with open(filename, 'rb') as f:
        try:  # catch OSError in case of a one line file
            f.seek(-2, os.SEEK_END)
            while f.read(1) != b'\n':
                f.seek(-2, os.SEEK_CUR)
        except OSError:
            f.seek(0)
        return f.readline().decode(encoding='utf-8')


class BaseDestination:

    def __init__(self, destination, streams):
        self.destination = destination
        self.streams = streams

    def get_state(self):
        raise NotImplementedError()

    def run(self, messages):
        raise NotImplementedError()


class LocalJsonDestination(BaseDestination):

    def __init__(self, destination, streams):
        super().__init__(destination, streams)
        self.states_file = f'{destination}/states.jsonl'
        self.logs_file = f'{destination}/logs.jsonl'
        self.stream_file = lambda stream: f'{destination}/{stream}.jsonl'
        create_file_or_try_to_open(self.states_file)
        create_file_or_try_to_open(self.logs_file)
        for stream in streams:
            create_file_or_try_to_open(self.stream_file(stream))

    def get_state(self):
        content = get_latest_line_of_file(self.states_file)
        if not content:
            return {}
        else:
            state = json.loads(content)
            return state['data']

    def run(self, messages):
        states_file = open(self.states_file, 'a', encoding='utf-8')
        logs_file = open(self.logs_file, 'a', encoding='utf-8')
        streams_files = {
            stream: open(self.stream_file(stream), 'a', encoding='utf-8')
            for stream in self.streams
        }
        try:
            for message in messages:
                if message.type == airbyte_cdk.models.Type.LOG:
                    message = message.log.json(exclude_unset=True)
                    print(message)
                    logs_file.write(message + '\n')
                elif message.type == airbyte_cdk.models.Type.RECORD:
                    file = streams_files[message.record.stream]
                    row = json.dumps(message.record.data)
                    file.write(row + '\n')
                elif message.type == airbyte_cdk.models.Type.STATE:
                    states_file.write(message.state.json(exclude_unset=True) + '\n')
        finally:
            states_file.close()
            logs_file.close()
            for stream_file in streams_files.values():
                stream_file.close()
