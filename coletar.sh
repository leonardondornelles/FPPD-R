#!/bin/sh
# Roda os experimentos do Problema 2 e salva as saídas em resultados/.
# Uso, na raiz do repositório: sh coletar.sh
mkdir -p resultados
go version > resultados/ambiente.txt
go build -o /tmp/t1-pc ./produtor-consumidor || exit 1
/tmp/t1-pc -bench -p 4 -c 4 -n 125000 | tee resultados/bench_com_timeout.txt
/tmp/t1-pc -bench -p 4 -c 4 -n 125000 -timeout 0 | tee resultados/bench_sem_timeout.txt
rm -f /tmp/t1-pc
