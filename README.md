# big-data-project

###docker-comand

Run docker with volumes
```
docker run -it --rm --name app --mount source=big-data-project_app_data,target=/app app
```
Copy data to docker container(volumse)
```
docker cp ./ app:/app
```

Copy res from docker container
```
docker cp app:/app/big-data-project/pukhta ./queries 
```