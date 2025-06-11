from setuptools import setup, find_packages

setup(
    name="autosinapi",
    version="0.1",
    packages=find_packages(where="."),
    package_dir={"": "."},
    install_requires=[
        'numpy',
        'openpyxl',
        'pandas',
        'requests',
        'setuptools',
        'sqlalchemy',
        'tqdm',
        'typing',
    ],
    python_requires='>=3.0',
)