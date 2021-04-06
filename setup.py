from setuptools import setup


def parse_requirements(*files):
    required = []
    for file in files:
        with open(file) as f:
            required.append(f.read().splitlines())
    return required


setup(
    install_requires=parse_requirements('requirements.txt'),
)
