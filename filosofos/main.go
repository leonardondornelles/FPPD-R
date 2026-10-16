// Jantar dos Filósofos com N filósofos e R refeições cada.
//
// Cada garfo é um canal com buffer 1: pegar o garfo é enviar um valor
// (bloqueia se o garfo já está com alguém) e soltar é receber o valor de volta.
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type mesa struct {
	n, r       int
	estrategia string
	garfos     []chan struct{}
	lugares    chan struct{} // conductor: só n-1 filósofos sentados ao mesmo tempo
	done       chan struct{} // fechado pelo watchdog para cancelar a mesa
	refeicoes  atomic.Int64  // total de refeições, usado pelo watchdog para ver progresso

	// barreira da versão deadlock: só abre quando todos pegaram o garfo esquerdo
	comEsquerdo atomic.Int64
	todosComEsq chan struct{}
}

type dados struct {
	refeicoes   int
	esperaTotal time.Duration
	esperaMax   time.Duration
}

// pegar tenta pegar o garfo, mas desiste se a mesa for cancelada.
// Sem o select, um filósofo em deadlock ficaria bloqueado para sempre.
func (m *mesa) pegar(garfo chan struct{}) bool {
	select {
	case garfo <- struct{}{}:
		return true
	case <-m.done:
		return false
	}
}

func (m *mesa) pegarGarfos(id int) bool {
	esq, dir := id, (id+1)%m.n

	switch m.estrategia {
	case "deadlock":
		// Todos pegam o esquerdo e só tentam o direito depois que TODOS estão com
		// o esquerdo na mão. A barreira garante a espera circular em qualquer
		// execução (um Sleep só tornaria o cenário provável).
		if !m.pegar(m.garfos[esq]) {
			return false
		}
		if m.comEsquerdo.Add(1) == int64(m.n) {
			close(m.todosComEsq) // o último a pegar o esquerdo abre a barreira
		}
		select {
		case <-m.todosComEsq:
		case <-m.done:
			return false
		}
		return m.pegar(m.garfos[dir])

	case "hierarchy":
		// Sempre o garfo de menor índice primeiro. Só o último filósofo
		// (garfos n-1 e 0) inverte a ordem, e isso impede o ciclo.
		if esq > dir {
			esq, dir = dir, esq
		}
		return m.pegar(m.garfos[esq]) && m.pegar(m.garfos[dir])

	default: // conductor
		// Ocupa um dos n-1 lugares antes de pegar os garfos.
		select {
		case m.lugares <- struct{}{}:
		case <-m.done:
			return false
		}
		return m.pegar(m.garfos[esq]) && m.pegar(m.garfos[dir])
	}
}

func (m *mesa) soltarGarfos(id int) {
	<-m.garfos[id]
	<-m.garfos[(id+1)%m.n]
	if m.estrategia == "conductor" {
		<-m.lugares
	}
}

// Tempo de pensar ou comer: entre 5 e 15ms. Com tempos fixos todos andariam
// sincronizados e a medição de fairness não diria nada.
func sortear(rng *rand.Rand) time.Duration {
	return time.Duration(5+rng.Intn(11)) * time.Millisecond
}

func (m *mesa) filosofo(id int, d *dados, wg *sync.WaitGroup) {
	defer wg.Done()
	rng := rand.New(rand.NewSource(int64(id) + 1)) // um gerador por filósofo

	for i := 0; i < m.r; i++ {
		time.Sleep(sortear(rng)) // pensando

		inicio := time.Now()
		if !m.pegarGarfos(id) {
			return // mesa cancelada pelo watchdog
		}
		espera := time.Since(inicio)
		d.esperaTotal += espera
		if espera > d.esperaMax {
			d.esperaMax = espera
		}
		d.refeicoes++
		m.refeicoes.Add(1)

		time.Sleep(sortear(rng)) // comendo
		m.soltarGarfos(id)
	}
}

// watchdog olha a cada 100ms se alguém comeu. Se passar 2s sem nenhuma
// refeição, considera deadlock e fecha done para todos desistirem.
func (m *mesa) watchdog(fim chan struct{}) bool {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	ultimo, ultimaMudanca := int64(0), time.Now()

	for {
		select {
		case <-fim:
			return false
		case <-ticker.C:
			if atual := m.refeicoes.Load(); atual != ultimo {
				ultimo, ultimaMudanca = atual, time.Now()
			} else if time.Since(ultimaMudanca) > 2*time.Second {
				close(m.done)
				return true
			}
		}
	}
}

func main() {
	n := flag.Int("n", 5, "número de filósofos")
	r := flag.Int("r", 100, "refeições por filósofo")
	estrategia := flag.String("strategy", "hierarchy", "deadlock, hierarchy ou conductor")
	flag.Parse()

	if *n < 2 || *r < 1 {
		fmt.Fprintln(os.Stderr, "erro: use -n >= 2 e -r >= 1")
		os.Exit(2)
	}
	if *estrategia != "deadlock" && *estrategia != "hierarchy" && *estrategia != "conductor" {
		fmt.Fprintln(os.Stderr, "erro: estratégia deve ser deadlock, hierarchy ou conductor")
		os.Exit(2)
	}

	m := &mesa{
		n:           *n,
		r:           *r,
		estrategia:  *estrategia,
		garfos:      make([]chan struct{}, *n),
		lugares:     make(chan struct{}, *n-1),
		done:        make(chan struct{}),
		todosComEsq: make(chan struct{}),
	}
	for i := range m.garfos {
		m.garfos[i] = make(chan struct{}, 1)
	}

	fmt.Printf("N=%d R=%d estratégia=%s\n", *n, *r, *estrategia)

	fim := make(chan struct{})
	deadlock := make(chan bool, 1)
	go func() { deadlock <- m.watchdog(fim) }()

	d := make([]dados, *n) // cada filósofo só escreve na sua posição
	var wg sync.WaitGroup
	for i := 0; i < *n; i++ {
		wg.Add(1)
		go m.filosofo(i, &d[i], &wg)
	}

	// Encerramento: espera os filósofos, avisa o watchdog e espera ele sair.
	wg.Wait()
	close(fim)
	houveDeadlock := <-deadlock
	if houveDeadlock {
		fmt.Println("DEADLOCK: ninguém comeu nos últimos 2s.")
		fmt.Println("Cada filósofo segura o garfo esquerdo e espera o direito (espera circular).")
		fmt.Println("A mesa foi cancelada e todas as goroutines terminaram.")
		fmt.Println("As esperas em andamento foram interrompidas e não entram nas médias.")
	} else {
		fmt.Println("Todos os filósofos terminaram as refeições.")
	}

	fmt.Printf("\n%-9s %-10s %-14s %-14s\n", "Filósofo", "Refeições", "Espera média", "Espera máxima")
	var soma time.Duration
	for i, x := range d {
		if x.refeicoes == 0 {
			fmt.Printf("%-9d %-10d %-14s %-14s\n", i, 0, "-", "-")
			continue
		}
		media := x.esperaTotal / time.Duration(x.refeicoes)
		soma += media
		fmt.Printf("%-9d %-10d %-14v %-14v\n", i, x.refeicoes, media.Round(time.Microsecond), x.esperaMax.Round(time.Microsecond))
	}
	if !houveDeadlock {
		fmt.Printf("\nEspera média geral: %v\n", (soma / time.Duration(*n)).Round(time.Microsecond))
	}
}
