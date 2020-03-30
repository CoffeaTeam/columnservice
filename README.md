Development environment:
```bash
export X509_USER_PROXY=$(voms-proxy-info --path)
docker build -t coffeateam/coffea-columnservice:dev .
docker-compose up -d
```
