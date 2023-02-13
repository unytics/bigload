from setuptools import setup


setup(
    name='bigload',
    version='0.1.0',
    packages=['bigload'],
    include_package_data=False,
    install_requires=[
        'click',
        'click-help-colors',
    ],
    entry_points={
        'console_scripts': [
            'bigload = bigload.cli:cli',
        ],
    },
)
