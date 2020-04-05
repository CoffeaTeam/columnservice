FROM coffeateam/coffea-dask:0.1.19

COPY . /src/columnservice

RUN cd /src/columnservice && /opt/conda/bin/pip install --editable .[server] && cd /opt/app

RUN apt-get update \
    && apt-get install -y htop \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

EXPOSE 8000
