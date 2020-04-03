FROM coffeateam/coffea-dask:0.1.19

COPY . .

RUN /opt/conda/bin/pip install --editable .[server]

RUN apt-get update \
    && apt-get install -y htop \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

EXPOSE 8000
