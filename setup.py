from setuptools import setup

setup(
    name='deflux',
    version='0.1',
    py_modules=['deflux'],
    install_requires=[
        'Click',
    ],
    entry_points='''
        [console_scripts]
        deflux=deflux:extract_data
    ''',
)