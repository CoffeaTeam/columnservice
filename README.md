Build
```bash
docker build -t coffeateam/coffea-columnservice:0.1.x .
```

Create `env.sh` to test locally (in k8s this would be in the pod config.)
You will need to provide mongodb and dask host instances.
```bash
MONGODB_USERNAME=coffea
MONGODB_PASSWORD=asdf
MONGODB_HOSTNAME=host.docker.internal
MONGODB_DATABASE=coffeadb
DASK_SCHEDULER=tcp://host.docker.internal:8786
X509_USER_PROXY=/tmp/proxy
```

Run locally
```bash
docker run -p 8000:8000 -v $(voms-proxy-info --path):/tmp/proxy --env-file env.sh coffeateam/coffea-columnservice:0.1.x 
```
