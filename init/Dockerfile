FROM apache/kafka:3.7.0 AS kafka
FROM cassandra:4.1

COPY --from=kafka /opt/kafka /opt/kafka
ENV PATH="/opt/kafka/bin:${PATH}"

COPY init.sh /usr/local/bin/init.sh
RUN chmod +x /usr/local/bin/init.sh

ENTRYPOINT ["/usr/local/bin/init.sh"]