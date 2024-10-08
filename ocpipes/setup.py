from setuptools import setup

setup(
    name='ocpipes',
    version='0.0.0',
    packages=['ocpipes'],
    install_requires=[
        'boto3',
        'pandas',
        'cryptography',
        'slack_sdk',
        'pyarrow',
        'snowflake-connector-python',
        'pyyaml',
        'awswrangler',
        'metaflow'
    ],
    author='',
    author_email='',
    description='ocpipes package',
    long_description='ocpipes package',
    url='https://github.com/MoveRDC/ocpipes',
    license=' Licensed ',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
    ],
)

