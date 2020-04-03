FROM coffeateam/coffea-dask:0.1.19

COPY . .

RUN /opt/conda/bin/pip install --editable .[server]

EXPOSE 8000
