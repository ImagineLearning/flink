# Docker build

The files in this directory are used during the Maven build to prepare for building a Flink Docker image.  Maven
does everything but run the `docker build ...` command.  To create the docker image, follow these steps:

The file, `Dockerfile.template`, was created using a [modified version](https://github.com/ImagineLearning/flink-docker/pull/1/files) of the flink-docker project.

Steps to build the Docker image:
1. Run `mvn -DskipTests clean package`
2. Change to this directory: `cd target/flink-docker`
3. Run the docker build command: `docker build -t flink:<version> .`


