from setuptools import setup, find_packages

setup(
    name='conference_scheduling',
    python_requires='>=3.6.1',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    entry_points={
        'console_scripts': [
            'schedule=conference_scheduling.__main__:main'
        ]
    },
    install_requires=[
        'numpy==1.18.1',
        'pandas==1.0.0',
        'openpyxl==3.0.3',
        'xlrd==1.2.0'
    ]
)
