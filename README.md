For the project installation, you can follow these steps:

1. You need to first have install Docker and Docker compose. If your computer haven't installed Docker and Docker compose, you can refer to this link for the installation [link1](https://docs.docker.com/engine/install/ubuntu/), [link2](https://docs.docker.com/compose/install/).

2. You need to create folder for every volumes section in the docker-compose.yml file. You also need to set hostname for the ip of all container in your computer too.

3. Run `docker-compose up -d` in the main project.

4. Create a virtual python env (should be python 3.10.6), activate it and run `pip install -r requirements.txt`.

5. Run `python3 kafka/amz_producer/app.py`.

6. Run `python3 kafka/amz_consumer/app.py`.

7. Run `python3 -m spark.model_trainer.app`.

8. Now your project should start and run smoothly.

For the Kibana dashboard. Please contact to me to get them. Thank you.
