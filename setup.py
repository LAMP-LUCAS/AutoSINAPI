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
        'pytest>=7.0.0',
        'pytest-mock>=3.10.0',
        'pytest-cov>=4.0.0',
    ],
    python_requires='>=3.8',  # Atualizado para versão mais moderna
    author="Lucas Antonio M. Pereira",
    author_email="contato@arqlamp.com",
    description="Toolkit para automação do SINAPI",
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
)