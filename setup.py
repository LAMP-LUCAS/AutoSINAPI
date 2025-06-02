from setuptools import setup, find_packages

setup(
    name="autosinapi",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        'pandas',
        'openpyxl',
        # outras dependências
    ],
    python_requires='>=3.8',
)

entry_points={
    'console_scripts': [
        'sinapi-insert=AutoSINAPIpostgres.tools.sql_sinapi_insert_2025:main',
    ],
}