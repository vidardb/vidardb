# Run PostgreSQL with VidarDB in Docker

We can easily deploy PostgreSQL with VidarDB in a Docker container.

## Get Started

The following document will guide you to install and play with VidarDB in a few minutes:

- Docker is the only requirement. Install it at https://docs.docker.com/install/ .

- Run a VidarDB Docker container:

    ```sh
    # Before running it, please ensure that both the container name 
    # 'postgresql' and the port '5432' have not been used in your environment.

    docker run -d --name postgresql -p 5432:5432 vidardb/postgresql:vidardb-latest
    ```

    After executing the above command, the `postgresql` container will start and the database will be initialized.
    Normally we should wait for 25~30s, and then the container's status will become `healthy`.

    Use the following command to check the status of `postgresql`:

    ```sh
    docker ps -f name=postgresql
    ```

- Connect to the PostgreSQL after the container is ready:

    ```sh
    psql -h 127.0.0.1 -p 5432 -U postgres
    ```

    A welcome from the PostgreSQL should appear.
    *Please note that the PostgreSQL client should already be installed before running the container.*

- For the users who don't have the PostgreSQL client installed:

    ```sh
    docker exec -it postgresql /bin/bash
    ```

    Then we can connect to the PostgreSQL successfully inside the container (retry the connecting command again).

## Building your own Docker image

You may want to create your custom Docker image. Do it by '1-click':

```sh
# Building Docker image with the default name 'vidardb/postgresql:vidardb-latest'

make docker-image
```

Some available build parameters:

```sh
REGISTRY=<YOUR REGISTRY ADDRESS> IMAGE=<YOUR IMAGE NAME> TAG=<YOUR IMAGE TAG> make docker-image 
```

## Enable AI support in your database

VidarDB loves AI! Our bulit-in database has already installed [MADlib](https://madlib.apache.org/) which supports big data machine learning in SQL. If you want to use it in the `postgresql` container, try the following command:

```sh
docker exec -it postgresql sh -c "install-madlib.sh -U <YOUR DATABASE USERNAME> -P <YOUR DATABASE PASSWORD> -D <YOUR DATABASE NAME>"
```

For example, if you have created a new database named `madlib`, and the username is `madlib` and there is no password,
then you can simply install madlib like this:

```sh
docker exec -it postgresql sh -c "install-madlib.sh -U madlib -D madlib"
```
