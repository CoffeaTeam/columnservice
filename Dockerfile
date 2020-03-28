FROM continuumio/miniconda3:4.8.2

RUN apt-get install gnupg2 -y \
    && wget -q -O - https://dist.eugridpma.info/distribution/igtf/current/GPG-KEY-EUGridPMA-RPM-3 | apt-key add - \
    && echo "deb http://repository.egi.eu/sw/production/cas/1/current egi-igtf core" >> /etc/apt/sources.list \
    && apt-get update \
    && apt-get install -y ca-policy-egi-core \
    && apt-get purge -y gnupg2 \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

RUN conda install --yes --freeze-installed -c conda-forge \
    distributed==2.12.0 \
    lz4==3.0.2 \
    tini==0.18.0 \
    xrootd==4.11.2 \
    && conda clean -afy

RUN /opt/conda/bin/pip install --no-cache-dir \
    xxhash==1.3.0 \
    motor==2.1.0 \
    fastapi==0.52.0 \
    uvicorn==0.11.3 \
    uproot==3.11.3 \
    https://github.com/nsmith-/dmwmclient/archive/v0.0.3.zip

RUN mkdir /opt/app

WORKDIR /opt/app

COPY . .

EXPOSE 8000

ENTRYPOINT ["tini", "-g", "--", "uvicorn", "--host", "0.0.0.0", "columnservice:api"]
