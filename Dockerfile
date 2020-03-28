FROM coffeateam/coffea-dask:0.1.x

RUN /opt/conda/bin/pip install --no-cache-dir \
    motor==2.1.0 \
    fastapi==0.52.0 \
    uvicorn==0.11.3 \
    https://github.com/nsmith-/dmwmclient/archive/v0.0.3.zip

COPY . .

EXPOSE 8000

ENTRYPOINT ["tini", "-g", "--", "uvicorn", "--host", "0.0.0.0", "columnservice:api"]
