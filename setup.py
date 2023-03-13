from setuptools import setup


setup(
    name='bigloader',
    version='0.1.0',
    packages=['.'],
    include_package_data=False,
    install_requires=[
        'click',
        'click-help-colors',
        'jinja2',
        'pyyaml',
        'airbyte-cdk',
        'google-cloud-bigquery',
    ],
    entry_points={
        'console_scripts': [
            'bigloader = bigloader.cli:cli',
        ],
    },
)
