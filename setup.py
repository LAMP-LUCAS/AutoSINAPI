from setuptools import setup, find_packages

setup(
    name="autosinapi",
    version="0.1",
    packages=find_packages(where="."),
    package_dir={"": "."},
    install_requires=[
        'pandas>=2.0',
        'openpyxl>=3.0',
        # Adicione apenas dependências externas aqui
    ],
    python_requires='>=3.8',
)