FROM ubuntu:20.04

RUN apt update -y && apt upgrade -y && apt install python3-pip -y
WORKDIR /code
ENV PYTHONPATH '/code/vendor'

ENTRYPOINT ["python3", "/code/vendor/PyInstaller/__main__.py", "--onefile", "--clean", "--noconfirm", "--name=ampm", "ampm/cli.py"]
