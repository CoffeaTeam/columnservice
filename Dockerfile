FROM coffeateam/coffea-dask:0.1.21

COPY . /src/columnservice

RUN cd /src/columnservice && /opt/conda/bin/pip install --no-cache-dir --editable .[server] && cd /opt/app

EXPOSE 8000
