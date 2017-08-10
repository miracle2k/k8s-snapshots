from setuptools import setup, find_packages

setup(
    name='k8s-snapshots',
    packages=find_packages(exclude=['tests']),
    entry_points={
        'console_scripts': [
            'k8s-snapshots=k8s_snapshots.__main__:main'
        ]
    }
)
