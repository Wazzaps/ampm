FROM ubuntu:20.04

RUN apt update -y && apt upgrade -y && apt install python3-pip -y
WORKDIR /code
ENV PYTHONPATH '/code/vendor'

ENTRYPOINT ["sh", "-c", "python3 /code/vendor/PyInstaller/__main__.py --clean --noconfirm --name=ampm ampm/cli.py && mv /code/dist/ampm/ampm /code/dist/ampm/ampm_py && cp /code/ampm/ampm.sh /code/dist/ampm/ampm"]
