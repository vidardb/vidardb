# Run VidarDB with Docker

We can easily deploy VidarDB with Docker. This section will guide you to install and play with VidarDB in one minute.

## Requirement

Docker is the only requirement. Install it at [https://docs.docker.com/install/](https://docs.docker.com/install/) .

## Run a VidarDB's Docker container

*Before running any of the following commands, please ensure that both the container name `vidardb` and the port `5432` have not been used in your environment.*

To run a VidarDB's Docker container:

```sh
docker run -d --name vidardb -p 5432:5432 vidardb/vidardb:latest
```

After doing that, a `vidardb` container will start and the database will be initialized.

## Connect to the VidarDB

For the users who don't have the PostgreSQL client installed:

```sh
docker exec -it vidardb sh -c 'psql -h 127.0.0.1 -p 5432 -U postgres'
```

For the others who have the PostgreSQL client installed:

```sh
psql -h 127.0.0.1 -p 5432 -U postgres
```

## Build your own Docker image

You may want to create your custom Docker image. Do it by '1-click' in the root directory of `vidardb` repository:

```sh
# Build a Docker image with the default name 'vidardb/vidardb:latest'
make docker-image
```

Some available build parameters:

```sh
REGISTRY=<YOUR REGISTRY ADDRESS> IMAGE=<YOUR IMAGE NAME> TAG=<YOUR IMAGE TAG> make docker-image 
```

## Enable AI support in your database

VidarDB loves AI! Our bulit-in database has already installed [MADlib](https://madlib.apache.org/) which supports big data machine learning in SQL. If you want to use it in the `vidardb` container, try the following command:

```sh
docker exec -it vidardb sh -c "install-madlib.sh -U <YOUR DATABASE USERNAME> -P <YOUR DATABASE PASSWORD> -D <YOUR DATABASE NAME>"
```

For example, if you have created a new database named `madlib`, and the username is `madlib` and there is no password,
then you can simply install madlib like this:

```sh
docker exec -it vidardb sh -c "install-madlib.sh -U madlib -D madlib"
```
