FROM confluentinc/cp-kafka:7.4.0

USER root

COPY create-topics.sh /create-topics.sh
RUN chmod +x /create-topics.sh

CMD ["/create-topics.sh"]
