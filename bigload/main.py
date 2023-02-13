import json
import os
import subprocess
import sys

from flask import Flask, request, jsonify

import conf

AVAILABLE_CONNECTORS = os.listdir(conf.AIRBYTE_CONNECTORS_FOLDER)
AVAILABLE_COMMANDS = ['spec', 'fake_configs', 'read', 'get_auto_configured_catalog', 'check_connection']
COMMANDS_NEEDING_CONFIG = ['read', 'get_auto_configured_catalog', 'check_connection']


app = Flask(__name__)


def get_python_command(connector_name, command, config):
    cmd = f'{conf.PYTHON} main_airbyte.py {connector_name} {command}'.format(connector_name=connector_name)
    if config is not None:
        config = json.dumps(config).replace('"', r'\"')
        cmd += f' --config "{config}"'
    return cmd


def read(cmd):
    outputs = []
    with open('test.log', 'w') as f:
        process = subprocess.Popen(cmd, stderr=subprocess.STDOUT, stdout=subprocess.PIPE, shell=True, encoding=sys.stdout.encoding)
        for line in iter(process.stdout.readline, ''):
            print(line)
            f.write(line)
            outputs.append(line)
    returncode = process.poll()

    for k in range(len(outputs)):
        outputs[k] = outputs[k].strip()
        try:
            outputs[k] = json.loads(outputs[k])
        except:
            break

    return outputs[0] if len(outputs) == 1 else outputs


@app.route('/source_connectors')
def source_connectors():
    return {
        connector_name: f'{request.url}/{connector_name}'
        for connector_name in AVAILABLE_CONNECTORS
    }


@app.route('/source_connectors/<connector_name>')
def source_connector(connector_name):
    if connector_name not in AVAILABLE_CONNECTORS:
        return f'connector "{connector_name}" not found', 404
    return {
        command: f'{request.url}/{command}'
        for command in AVAILABLE_COMMANDS
    }


@app.route('/source_connectors/<connector_name>/<command>')
def source_connector_command(connector_name, command):
    if connector_name not in AVAILABLE_CONNECTORS:
        return f'connector "{connector_name}" not found', 404
    if command not in AVAILABLE_COMMANDS:
        return f'command "{command}" unavailable', 404

    if command in COMMANDS_NEEDING_CONFIG:
        from connectors.destinations.google_cloud_storage import DestinationConnector
        dest_connector = DestinationConnector(conf.DESTINATION_BUCKET, connector_name)
        config = dest_connector.config
    else:
        config = None

    cmd = get_python_command(connector_name, command, config)
    if command == 'read':
        result = read(connector_name, config)
    else:
        job = subprocess.run(cmd, shell=True, capture_output=True, encoding=sys.stdout.encoding)
        if job.returncode != 0 and job.stderr:
            result = {'uncaught_error': job.stderr, 'output': job.stdout}
        else:
            try:
                result = json.loads(job.stdout)
            except:
                result = {'output': job.stdout}

    return jsonify(result)


@app.route('/')
def hello():
    return 'hey'



if __name__ == '__main__':
    app.run(debug=True)
