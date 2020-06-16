from setuptools import find_packages, setup

install_requires = []
dev_requires = install_requires + [
    "autopep8>=1.4.4",
    'black>=18.0.b0,<19;python_version>="3.6"',
    "flake8",
    "ipython",
    "isort>=4.3.21",
    "moto",
    "pip-tools",
    "pylint",
    "pytest",
    "pytest-cov",
    "pytest-mock",
]


setup(
    name="alluvium",
    author="Abhinava Singh",
    author_email="abhinavasinghweb@gmail.com",
    maintainer="Abhinava Singh",
    maintainer_email="abhinavasinghweb@gmail.com",
    description="",
    packages=find_packages("src"),
    package_dir={"": "src"},
    install_requires=install_requires,
    extras_require={"dev": dev_requires},
    # Currently `savage` support Python 2.7, and Python 3.6+
    python_requires=">=2.7, !=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*, !=3.4.*, !=3.5.*, <4",
    include_package_data=True,
)
