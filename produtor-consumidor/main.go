// Produtor/Consumidor com P produtores, C consumidores e buffer de capacidade K,
// em duas versões: -mode channel e -mode semaphore.
package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type config struct {
	modo           string
	p, c, k, n     int           // n = itens por produtor
	timeout        time.Duration // 0 = consumidor 0 sem timeout (para comparação)
	atrasoProdutor time.Duration
	log            bool
}

type resultado struct {
	produzido, consumido int64
	porConsumidor        []int
	timeouts             int
	duracao              time.Duration
	ocupacao             float64
	ok                   bool
}

func executar(cfg config) resultado {
	var buf Buffer
	if cfg.modo == "channel" {
		buf = novoBufferCanal(cfg.k)
	} else {
		buf = novoBufferSemaforo(cfg.k)
	}

	total := cfg.p * cfg.n
	var produzido, consumido atomic.Int64
	recebido := make([]atomic.Int32, total) // quantas vezes cada ID foi consumido
	porConsumidor := make([]int, cfg.c)     // cada consumidor só escreve na sua posição
	timeouts := 0                           // só o consumidor 0 escreve

	// Ocupação média: uma goroutine lê o tamanho do buffer em intervalos de
	// ~100µs (o sistema pode atrasar o ticker) enquanto os produtores trabalham.
	pararAmostra := make(chan struct{})
	amostraFim := make(chan float64)
	go func() {
		ticker := time.NewTicker(100 * time.Microsecond)
		defer ticker.Stop()
		soma, amostras := 0, 0
		for {
			select {
			case <-ticker.C:
				soma += buf.Len()
				amostras++
			case <-pararAmostra:
				if amostras == 0 {
					amostraFim <- 0
				} else {
					amostraFim <- float64(soma) / float64(amostras)
				}
				return
			}
		}
	}()

	inicio := time.Now()
	var wgProd, wgCons sync.WaitGroup

	for p := 0; p < cfg.p; p++ {
		wgProd.Add(1)
		go func(p int) {
			defer wgProd.Done()
			for i := 0; i < cfg.n; i++ {
				time.Sleep(cfg.atrasoProdutor)
				buf.Put(p*cfg.n + i) // IDs únicos: 0, 1, ..., P*n-1
				produzido.Add(1)
			}
		}(p)
	}

	for c := 0; c < cfg.c; c++ {
		wgCons.Add(1)
		go func(c int) {
			defer wgCons.Done()
			espera := time.Duration(0)
			if c == 0 {
				espera = cfg.timeout // o consumidor 0 usa select com time.After
			}
			for {
				item, ok, timeout := buf.Get(espera)
				if timeout {
					// Comportamento alternativo: registra e volta a esperar. Não
					// desiste, porque timeout não quer dizer que a produção acabou.
					timeouts++
					if cfg.log && timeouts <= 5 {
						fmt.Printf("[consumidor 0] nenhum item em %v (timeout %d), tentando de novo\n", espera, timeouts)
					}
					continue
				}
				if !ok {
					return
				}
				porConsumidor[c]++
				consumido.Add(1)
				recebido[item].Add(1)
			}
		}(c)
	}

	// Encerramento: espera os produtores, fecha o buffer e espera os
	// consumidores esvaziarem o que sobrou.
	wgProd.Wait()
	close(pararAmostra)
	ocupacao := <-amostraFim
	buf.Fechar(cfg.c)
	wgCons.Wait()
	duracao := time.Since(inicio) // inclui o encerramento (esvaziar o buffer)

	// Verificação: totais iguais, buffer vazio e cada ID consumido exatamente uma vez.
	ok := produzido.Load() == int64(total) && consumido.Load() == int64(total) && buf.Len() == 0
	for i := range recebido {
		if recebido[i].Load() != 1 {
			ok = false
			break
		}
	}

	return resultado{produzido.Load(), consumido.Load(), porConsumidor, timeouts, duracao, ocupacao, ok}
}

func (r resultado) throughput() float64 {
	return float64(r.consumido) / r.duracao.Seconds()
}

func imprimir(cfg config, r resultado) {
	fmt.Printf("\nModo=%s P=%d C=%d K=%d itens=%d\n", cfg.modo, cfg.p, cfg.c, cfg.k, cfg.p*cfg.n)
	for i, qtd := range r.porConsumidor {
		extra := ""
		if i == 0 {
			extra = fmt.Sprintf(" (%d timeouts)", r.timeouts)
		}
		fmt.Printf("  consumidor %d: %d itens%s\n", i, qtd, extra)
	}
	fmt.Printf("Tempo: %v\n", r.duracao.Round(time.Microsecond))
	fmt.Printf("Throughput: %.0f itens/s\n", r.throughput())
	fmt.Printf("Ocupação média do buffer: %.2f (K=%d)\n", r.ocupacao, cfg.k)
	fmt.Printf("totalProduzido(%d) == totalConsumido(%d): %v\n", r.produzido, r.consumido, r.produzido == r.consumido)
	fmt.Printf("Cada item consumido exatamente uma vez: %v\n", r.ok)
}

// bench roda as duas versões com K = 1, 10 e 100, 5 vezes cada, e mostra as médias.
func bench(cfg config) {
	tm := "timeout do consumidor 0: " + cfg.timeout.String()
	if cfg.timeout == 0 {
		tm = "sem timeout"
	}
	fmt.Printf("P=%d C=%d itens=%d, %s, média de 5 execuções\n\n", cfg.p, cfg.c, cfg.p*cfg.n, tm)
	fmt.Printf("%-10s %-4s %-18s %-8s %-10s %s\n", "Modo", "K", "Throughput (it/s)", "Desvio", "Ocupação", "Itens por consumidor (média)")
	for _, modo := range []string{"channel", "semaphore"} {
		for _, k := range []int{1, 10, 100} {
			cfg.modo, cfg.k = modo, k
			var ocup float64
			thr := make([]float64, 5)
			itens := make([]float64, cfg.c)
			for rep := 0; rep < 5; rep++ {
				r := executar(cfg)
				if !r.ok {
					fmt.Println("ERRO: verificação falhou")
					os.Exit(1)
				}
				thr[rep] = r.throughput()
				ocup += r.ocupacao
				for i, q := range r.porConsumidor {
					itens[i] += float64(q)
				}
			}
			media, desvio := mediaDesvio(thr)
			desvioPct := "N/A" // sem itens o throughput é 0 e o desvio relativo não existe
			if media > 0 {
				desvioPct = fmt.Sprintf("%.1f%%", 100*desvio/media)
			}
			fmt.Printf("%-10s %-4d %-18.0f %-8s %-10.2f", modo, k, media, desvioPct, ocup/5)
			for _, q := range itens {
				fmt.Printf(" %.1f", q/5)
			}
			fmt.Println()
		}
	}
}

// mediaDesvio devolve a média e o desvio padrão amostral.
func mediaDesvio(xs []float64) (float64, float64) {
	var soma float64
	for _, x := range xs {
		soma += x
	}
	media := soma / float64(len(xs))
	var q float64
	for _, x := range xs {
		q += (x - media) * (x - media)
	}
	return media, math.Sqrt(q / float64(len(xs)-1))
}

func main() {
	var cfg config
	flag.StringVar(&cfg.modo, "mode", "channel", "channel ou semaphore")
	flag.IntVar(&cfg.p, "p", 4, "número de produtores")
	flag.IntVar(&cfg.c, "c", 4, "número de consumidores")
	flag.IntVar(&cfg.k, "k", 10, "capacidade do buffer")
	flag.IntVar(&cfg.n, "n", 1000, "itens por produtor")
	flag.DurationVar(&cfg.timeout, "timeout", 10*time.Millisecond, "timeout do consumidor 0 (0 desliga)")
	flag.DurationVar(&cfg.atrasoProdutor, "producer-delay", 0, "pausa antes de cada item produzido")
	rodarBench := flag.Bool("bench", false, "compara channel e semáforos com K = 1, 10 e 100")
	flag.Parse()

	if (cfg.modo != "channel" && cfg.modo != "semaphore") || cfg.p < 1 || cfg.c < 1 ||
		cfg.k < 1 || cfg.n < 0 || cfg.timeout < 0 || cfg.atrasoProdutor < 0 {
		fmt.Fprintln(os.Stderr, "erro: parâmetros inválidos (use -h para ver as opções)")
		os.Exit(2)
	}

	if *rodarBench {
		bench(cfg)
		return
	}

	cfg.log = true
	r := executar(cfg)
	imprimir(cfg, r)
	if !r.ok {
		os.Exit(1)
	}
}
