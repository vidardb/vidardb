# Run PostgreSQL with VidarDB in Docker

We can deploy PostgreSQL with VidarDB in Docker container.

## Testing

Start the container with the following docker image we have provided.

- Install docker engine:

    Docker engine is available on multiple platforms and just follow the [official doc](https://docs.docker.com/install/#supported-platforms) to choose the best installation option for you.

- Run a docker container:

    ```sh
    # we should ensure that both the container name 'postgresql' and the port '5432' 
    # have not been used in our environment

    docker run -d --name postgresql -p 5432:5432 vidardb/postgresql:vidardb-latest
    ```

    Normally we should wait for 25~30s, the `postgresql` container will be started, and the status
    will become to `healthy`.

    We can run the following command to query the `postgresql` container status:

    ```sh
    docker ps -f name=postgresql
    ```

- Connect to the PostgreSQL after the container is `healthy`:

    ```sh
    psql -h 127.0.0.1 -p 5432 -U postgres
    ```

    Please note that PostgreSQL client should already be installed before running the container.

## Building

We can build a new docker image in the following way. It is the prerequisite that install docker engine in the building machine.

```sh
# building docker image with default image repository and name 'vidardb/postgresql:vidardb-latest'

make docker-image
```

We can also specify the build parameters:

```sh
REGISTRY=<YOUR REGISTRY ADDRESS> IMAGE=<YOUR IMAGE NAME> TAG=<YOUR IMAGE TAG> make docker-image 
```

## Installing MADLib

If you need to install MADLib for a new created database in `postgresql` container, you can run the following instruction:

```sh
docker exec -it postgresql sh -c "install-madlib.sh -U <YOUR DATABASE USERNAME> -P <YOUR DATABASE PASSWORD> -D <YOUR DATABASE NAME>"
```

For example, if you have created a new database named `madlib`, and the username is `madlib` and no password,
then you can install madlib like this:

```sh
docker exec -it postgresql sh -c "install-madlib.sh -U madlib -D madlib"
```
