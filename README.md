### columnservice: a multi-tenant service for caching columnar data 

![Untitled(2)](https://user-images.githubusercontent.com/801118/111381882-4d87b600-8674-11eb-9e8e-43d91b722c8b.png)


Development environment:
```bash
export X509_USER_PROXY=$(voms-proxy-info --path)
docker build -t coffeateam/coffea-columnservice:dev .
docker-compose up -d
```

Shell in environment: `docker-compose exec jupyter bash`
