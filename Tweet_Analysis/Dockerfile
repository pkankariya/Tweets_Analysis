FROM openjdk:10-jre-slim
MAINTAINER Preetam Dedu (myemail@gmail.com)
RUN apt-get update
RUN apt-get install -y maven
COPY pom.xml /usr/local/service/pom.xml
COPY src /usr/local/service/src
COPY twitter_data.json /usr/local/service/twitter_data.json
COPY entrypoint.sh /usr/local/service/
RUN chmod +x /usr/local/service/entrypoint.sh
WORKDIR /usr/local/service
RUN mvn package
ENTRYPOINT ["/usr/local/service/entrypoint.sh"]
