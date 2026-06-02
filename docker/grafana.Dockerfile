FROM grafana/grafana:10.4.13

RUN grafana-cli plugins install vertamedia-clickhouse-datasource
