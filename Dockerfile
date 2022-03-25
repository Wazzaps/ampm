FROM ubuntu:20.04

RUN apt update -y && apt upgrade -y && apt install python3-pip -y
WORKDIR /code
ENV PYTHONPATH '/code/vendor'

ENTRYPOINT ["sh", "-c", \
"python3 /code/vendor/PyInstaller/__main__.py --clean --noconfirm --name=ampm ampm/cli.py && \
cp /code/ampm/ampm.sh /code/dist/ampm/ampm.sh && \
cd /code/dist/ampm && tar -czvf /code/dist/ampm.tar.gz ." \
]
