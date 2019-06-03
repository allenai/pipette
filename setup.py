from setuptools import setup, find_packages

setup(
  name='pipette',
  version='1.3.0',
  description='Python framework for managing experiment workflows',
  url='https://github.com/allenai/pipette',
  packages=find_packages(),
  py_modules=['pipette'],
  test_suite="tests",
  install_requires=[
    'dill',
    'mmh3',
    'typeguard',
    'regex>=2019.06.02'
  ],
  python_requires='>=3.6'
)
