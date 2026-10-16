package main

import (
	"sync"
	"sync/atomic"
	"time"
)

// Buffer é a interface comum das duas versões. Produtores, consumidores e
// medições são o mesmo código nos dois modos; só a implementação do buffer muda.
type Buffer interface {
	Put(item int)
	// Get devolve (item, ok, timeout). ok == false quando não há mais itens.
	// Com espera > 0, desiste depois desse tempo e devolve timeout == true.
	Get(espera time.Duration) (int, bool, bool)
	Len() int
	Fechar(consumidores int) // chamado depois que todos os produtores terminaram
}

// ---------- Versão com channel ----------

type bufferCanal struct {
	ch chan int
}

func novoBufferCanal(k int) *bufferCanal {
	return &bufferCanal{ch: make(chan int, k)}
}

func (b *bufferCanal) Put(item int) { b.ch <- item } // bloqueia se cheio

func (b *bufferCanal) Get(espera time.Duration) (int, bool, bool) {
	if espera == 0 {
		item, ok := <-b.ch // bloqueia se vazio
		return item, ok, false
	}
	select {
	case item, ok := <-b.ch:
		return item, ok, false
	case <-time.After(espera):
		return 0, true, true
	}
}

func (b *bufferCanal) Len() int { return len(b.ch) }

// Fechar fecha o canal. Os consumidores continuam recebendo o que sobrou e só
// recebem ok == false quando o canal está fechado E vazio.
func (b *bufferCanal) Fechar(int) { close(b.ch) }

// ---------- Versão com semáforos ----------

// semaforo de contagem feito com canal: cada valor no canal é uma ficha.
type semaforo chan struct{}

func novoSemaforo(capacidade, fichas int) semaforo {
	s := make(semaforo, capacidade)
	for i := 0; i < fichas; i++ {
		s <- struct{}{}
	}
	return s
}

func (s semaforo) P() { <-s }             // pega uma ficha (espera se não houver)
func (s semaforo) V() { s <- struct{}{} } // devolve uma ficha

const sentinela = -1 // item especial que manda o consumidor parar

type bufferSemaforo struct {
	itens    []int
	in, out  int
	notFull  semaforo // fichas = posições livres (começa com K)
	notEmpty semaforo // fichas = itens disponíveis (começa com 0)
	mutexIn  sync.Mutex
	mutexOut sync.Mutex
	qtd      atomic.Int64 // só para medir a ocupação
}

func novoBufferSemaforo(k int) *bufferSemaforo {
	return &bufferSemaforo{
		itens:    make([]int, k),
		notFull:  novoSemaforo(k, k),
		notEmpty: novoSemaforo(k, 0),
	}
}

func (b *bufferSemaforo) Put(item int) {
	b.notFull.P()
	b.mutexIn.Lock() // dois produtores não escrevem na mesma posição
	b.itens[b.in] = item
	b.in = (b.in + 1) % len(b.itens)
	b.mutexIn.Unlock()
	b.qtd.Add(1)
	b.notEmpty.V()
}

func (b *bufferSemaforo) Get(espera time.Duration) (int, bool, bool) {
	if espera == 0 {
		b.notEmpty.P()
	} else {
		select {
		case <-b.notEmpty: // P(notEmpty) com limite de tempo
		case <-time.After(espera):
			return 0, true, true
		}
	}
	b.mutexOut.Lock() // dois consumidores não retiram o mesmo item
	item := b.itens[b.out]
	b.out = (b.out + 1) % len(b.itens)
	b.mutexOut.Unlock()
	b.qtd.Add(-1)
	b.notFull.V()

	if item == sentinela {
		return 0, false, false
	}
	return item, true, false
}

func (b *bufferSemaforo) Len() int { return int(b.qtd.Load()) }

// Fechar coloca uma sentinela por consumidor. O buffer é FIFO e a produção já
// acabou, então todos os itens reais saem antes das sentinelas.
func (b *bufferSemaforo) Fechar(consumidores int) {
	for i := 0; i < consumidores; i++ {
		b.Put(sentinela)
	}
}
