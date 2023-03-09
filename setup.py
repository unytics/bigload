from setuptools import setup


setup(
    name='bigload',
    version='0.1.0',
    packages=['.'],
    include_package_data=False,
    install_requires=[
        'click',
        'click-help-colors',
        'jinja2',
        'pyyaml',
        'airbyte-cdk',
    ],
    entry_points={
        'console_scripts': [
            'bigload = bigload.cli:cli',
        ],
    },
)
