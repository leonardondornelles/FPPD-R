#!/bin/sh
# Roda os casos de validação com o detector de corridas (-race).
# Uso, na raiz do repositório: sh validar.sh
falhas=0
casos=0
falha() { echo "FALHA: $1"; falhas=$((falhas + 1)); }

go vet ./... || falha "go vet"
go build -race -o /tmp/t1-fil ./filosofos || exit 1
go build -race -o /tmp/t1-pc ./produtor-consumidor || exit 1

echo "Filósofos: 3 estratégias x N = 2, 5, 20"
for e in deadlock hierarchy conductor; do
  for n in 2 5 20; do
    casos=$((casos + 1))
    saida=$(/tmp/t1-fil -strategy $e -n $n -r 20 2>&1) || { falha "filosofos $e n=$n"; continue; }
    if [ $e = deadlock ]; then esperado="DEADLOCK"; else esperado="terminaram as refei"; fi
    echo "$saida" | grep -q "$esperado" || falha "filosofos $e n=$n"
  done
done

echo "Produtor/consumidor: 2 modos x K = 1, 2, 10, 100 x (P,C) x itens = 0, 1, 500"
for modo in channel semaphore; do
  for k in 1 2 10 100; do
    for pc in "1 1" "3 7" "4 4" "8 2"; do
      set -- $pc
      for n in 0 1 500; do
        casos=$((casos + 1))
        saida=$(/tmp/t1-pc -mode $modo -k $k -p $1 -c $2 -n $n -timeout 1ms 2>&1) || { falha "$modo k=$k p=$1 c=$2 n=$n"; continue; }
        echo "$saida" | grep -q "exatamente uma vez: true" || falha "$modo k=$k p=$1 c=$2 n=$n"
      done
    done
  done
done

echo "Timeout com produtor lento e consumidor sem timeout"
for modo in channel semaphore; do
  casos=$((casos + 1))
  saida=$(/tmp/t1-pc -mode $modo -p 1 -c 2 -n 10 -producer-delay 20ms -timeout 2ms 2>&1) || falha "timeout $modo"
  echo "$saida" | grep -q "timeout 1" && echo "$saida" | grep -q "exatamente uma vez: true" || falha "timeout $modo"
  casos=$((casos + 1))
  /tmp/t1-pc -mode $modo -timeout 0 -n 200 >/dev/null 2>&1 || falha "sem timeout $modo"
done

echo "Parâmetros inválidos devem ser recusados"
for args in "-n 1" "-r 0" "-strategy x"; do
  casos=$((casos + 1))
  /tmp/t1-fil $args >/dev/null 2>&1 && falha "filosofos aceitou $args"
done
for args in "-k 0" "-p 0" "-c 0" "-n -1" "-mode x" "-timeout -1ms"; do
  casos=$((casos + 1))
  /tmp/t1-pc $args >/dev/null 2>&1 && falha "produtor-consumidor aceitou $args"
done

rm -f /tmp/t1-fil /tmp/t1-pc
echo "$casos casos, $falhas falhas"
[ $falhas -eq 0 ]
