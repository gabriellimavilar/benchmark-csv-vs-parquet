# Briefing para Apresentação — Otimização de Consultas Analíticas (CSV vs Parquet)

> **Instruções para a IA que vai montar a apresentação:** este arquivo é autossuficiente.
> Contém o tema, a justificativa das decisões técnicas, a explicação completa do código e
> todos os resultados medidos. Monte uma apresentação de **10 a 14 slides** a partir dele.
> Todos os números aqui são reais, medidos na máquina do autor — não invente nem arredonde
> para números "mais bonitos". Ao final há uma sugestão de roteiro de slides e um bloco de
> perguntas e respostas prováveis da banca.
>
> Trabalho: **Trilha B — Otimização de Consultas Analíticas**
> Linguagem: Python 3.14 · Autor: Gabriel · Plataforma: Windows 11

---

## 1. O tema escolhido e por que ele importa

### O problema de negócio

Uma empresa de e-commerce guarda todo o histórico de vendas (~1 milhão de registros) em
arquivos **CSV**. Os relatórios analíticos — faturamento por categoria, desempenho por
região, ranking de clientes — ficaram lentos. A pergunta que o trabalho responde é:

> **Migrar esse histórico de CSV para Parquet reduz de fato o tempo de consulta e o consumo
> de memória? E se reduz, de onde exatamente vem o ganho?**

A segunda metade da pergunta é a parte que diferencia o trabalho. É fácil mostrar que
"Parquet é mais rápido". O objetivo aqui foi **isolar e medir o mecanismo** de cada
otimização, para que a recomendação arquitetural tenha base empírica e não repetição de
senso comum.

### Os dois formatos, em uma frase cada

- **CSV** é texto puro, **orientado a linha** e **sem tipos**. Para ler qualquer coisa, é
  preciso ler tudo e converter texto em número/data. Não existe "ler meio CSV": sem índice
  e sem metadados, qualquer byte do arquivo pode conter a linha que interessa.
- **Parquet** é binário, **orientado a coluna** e **com schema**. Cada coluna é um bloco
  independente no arquivo, e um rodapé (*footer*) diz em que posição cada bloco começa e
  qual o `min`/`max` de cada coluna em cada bloco de linhas.

Essa diferença estrutural habilita duas otimizações que o CSV não pode oferecer:

| Otimização | O que faz | Como se escreve em PyArrow |
|---|---|---|
| **Column Projection** | lê só as colunas que a consulta usa; as outras nunca saem do disco | `columns=['region', 'order_value']` |
| **Predicate Pushdown** | empurra o `WHERE` para dentro do leitor, que descarta blocos e linhas antes de materializar | `filters=[('payment_method', '==', 'Pix')]` |

### Por que isso é relevante para Engenharia de Dados

A escolha de formato na camada de ingestão de um Data Lake é decidida uma vez e paga
milhares de vezes. Cada consulta que ainda toca CSV paga de novo o custo de parse. Em
serviços de nuvem cobrados por **bytes escaneados** (AWS Athena, Google BigQuery), o
formato é literalmente uma linha na fatura.

---

## 2. Visão geral da solução

```
Trabalho_MI/
├── data/
│   ├── dados_vendas.csv        76,5 MB   (1.000.000 registros, 10 colunas)
│   └── dados_vendas.parquet    22,8 MB   (snappy, 10 row groups)
├── results/
│   └── RESULTS.md              relatório gerado automaticamente (333 linhas)
├── script_benchmark.py         código principal (~1.170 linhas)
├── README.md                   instruções de uso
└── requirements.txt
```

Um único comando (`python script_benchmark.py`) executa o pipeline inteiro: gera os dados
se faltarem, roda o benchmark, valida os resultados e escreve o relatório com os números
da máquina local. Leva cerca de 5 minutos.

**A arquitetura do experimento:** 4 consultas × 3 estratégias de leitura = 12 medições de
tempo e 12 de memória, mais um experimento extra de isolamento com 4 variantes.

| Estratégia | Como lê | O que isso custa |
|---|---|---|
| **A. CSV + pandas** | `pd.read_csv()` do arquivo inteiro | 76 MB de texto + parse das 10 colunas, inclusive as 8 que a consulta não usa |
| **B. Parquet + PyArrow (sem otimização)** | `pq.read_table()` da tabela inteira | binário já tipado (sem parse), mas todas as 10 colunas vão para a RAM |
| **C. Parquet + PyArrow (com otimização)** | `columns=` + `filters=` | só as colunas necessárias, só as linhas que passam no filtro |

---

## 3. Explicação do código, parte por parte

O arquivo `script_benchmark.py` está dividido em 6 seções numeradas. Abaixo, o que cada
uma faz e **por que foi feita assim** — a justificativa é o que vale apresentar.

### Seção 1 — Instrumentação: medir tempo e memória

Esta é a seção com as decisões metodológicas mais importantes, e **duas delas corrigem
erros que um benchmark ingênuo comete**.

#### Medição de tempo: mediana, não mínimo

```python
def medir_tempo(funcao, repeticoes=9):
    tempos = [t * 1000.0 for t in timeit.repeat(funcao, repeat=repeticoes, number=1)]
    return {"mediana": statistics.median(tempos), "minimo": min(tempos), ...}
```

`number=1` porque cada execução já é caríssima (lê o arquivo inteiro); `repeat=9` para
amostrar a variação.

**Por que mediana e não o mínimo** (que é a prática comum em microbenchmarks): as leituras
Parquet duram apenas dezenas de milissegundos e oscilam muito — o desvio-padrão medido
chegou a **45% da própria mediana**. Nesse regime de ruído o mínimo é instável: entre
execuções do benchmark ele chegou a **inverter a ordem entre variantes**, o que produziria
uma conclusão diferente a cada rodada. A mediana é robusta a outliers nas duas pontas e
representa o tempo típico. O relatório publica o desvio-padrão junto, e avisa
explicitamente que diferenças menores que ~20% entre duas variantes de Parquet não devem
ser lidas como significativas.

#### Medição de memória: subprocesso limpo

```python
def medir_memoria(alvo: str) -> float:
    """Mede o pico de memoria de uma consulta num SUBPROCESSO limpo."""
    processo = subprocess.run([sys.executable, __file__, "--medir", alvo], ...)
```

**O erro que isso corrige — e vale destacar na apresentação.** A primeira versão media
`RSS_depois − RSS_antes` dentro do mesmo processo, e os resultados eram absurdos: a
primeira consulta media 180 MB e todas as seguintes mediam ~0 MB; em alguns casos a versão
"com otimização" aparecia gastando *mais* que a versão sem.

A causa: **o RSS de um processo não encolhe depois do primeiro pico**. O alocador de
memória devolve os blocos liberados para reuso interno, não para o sistema operacional.
Então, da segunda medição em diante, a memória já estava "pré-aquecida" e o delta lia zero.

A solução foi medir **cada consulta em um interpretador Python recém-criado**, de modo que
todas partam do mesmo estado de heap. O valor reportado é o pico de *working set* acima da
linha de base (interpretador já com as bibliotecas importadas), usando `peak_wset` do
`psutil` no Windows — que é o pico exato registrado pelo próprio sistema operacional. Em
outras plataformas, uma thread dedicada amostra o RSS a cada 2 ms.

```python
class MonitorMemoria(threading.Thread):
    """Amostra o RSS em background para capturar o PICO, não o valor final."""
    def run(self):
        while not self._parar.is_set():
            self.pico_bytes = max(self.pico_bytes, self._processo.memory_info().rss)
            time.sleep(self._intervalo)
```

O monitor em thread existe porque `psutil` expõe apenas o uso **instantâneo**, e o pico de
uma consulta acontece *no meio* da execução — por exemplo, com o DataFrame inteiro
carregado, antes de o filtro reduzi-lo.

### Seção 2 — Geração do dataset sintético

```python
rng = np.random.default_rng(seed=42)   # seed fixa -> dataset reproduzível
```

**Tudo vetorizado, zero loops Python.** Cada uma das 10 colunas sai de uma única chamada
numpy sobre um array de 1 milhão de elementos. Gera em cerca de 1 segundo; o mesmo em loop
levaria minutos. Este era um requisito explícito do trabalho.

Três decisões de modelagem que valem explicar:

**(a) `order_value` vem de distribuição lognormal, não uniforme.**

```python
order_value = np.round(rng.lognormal(mean=4.0, sigma=0.9, size=n), 2)
order_value = np.clip(order_value, 5.0, 25_000.0)
```

Valor de pedido no mundo real é assimétrico: muitos pedidos pequenos e poucos muito
grandes. Uma distribuição uniforme produziria um dataset irrealista. Efeito colateral
útil: cerca de 25% dos pedidos passam do filtro de R$ 100 da Q1 — um corte seletivo
realista, nem trivial nem vazio.

**(b) As categorias e regiões têm pesos desiguais.**

```python
REGIOES = ["Norte", "Nordeste", "Centro-Oeste", "Sudeste", "Sul"]
PESO_REGIOES = [0.07, 0.22, 0.09, 0.47, 0.15]   # proporção próxima do consumo real
```

Sudeste concentra quase metade dos pedidos, como no e-commerce brasileiro real. Isso
também torna a compressão por dicionário do Parquet mais representativa.

**(c) Os dados são gravados ORDENADOS por `sale_date`. Esta é a decisão mais importante
do trabalho inteiro.**

```python
offsets = np.sort(rng.integers(0, total_segundos, size=n, dtype=np.int64))
sale_date = inicio + offsets.astype("timedelta64[s]")
```

Parquet guarda `min`/`max` de cada coluna em cada *row group*. Se as datas estivessem
embaralhadas, **todo row group conteria o período inteiro** (2023 a 2024), seus intervalos
`[min, max]` se sobreporiam completamente, e o filtro de data **não conseguiria descartar
nenhum bloco** — o Predicate Pushdown por data ficaria inútil.

Ordenando cronologicamente, cada row group cobre uma janela estreita de tempo, e o filtro
`sale_date > 2024-01-01` descarta **4 dos 10 row groups** apenas lendo o rodapé, sem
descompactar nada. Isso também corresponde ao formato natural de um histórico acumulado
por ETL: dado de venda chega em ordem cronológica.

#### A gravação

```python
pq.write_table(tabela, ARQ_PARQUET,
               compression="snappy",
               row_group_size=100_000,      # 10 row groups de 100k linhas
               write_statistics=True)       # min/max por row group -> habilita pushdown
```

**Por que `row_group_size=100_000`:** o padrão do PyArrow criaria **um único row group** de
1 milhão de linhas — e com um bloco só, não há nada para o pushdown podar. Dez blocos dão
granularidade de poda. O alvo clássico de mercado é ~128 MB descomprimidos por row group;
grandes demais matam o pushdown, pequenos demais inflam o footer e fragmentam o I/O.

**Por que Snappy:** compressão rápida, padrão de fato em data lakes. Prioriza velocidade de
descompressão sobre taxa máxima — o que importa quando o arquivo é lido muitas vezes.

### Seção 3 — As 4 consultas analíticas (3 implementações cada)

Convenção do código: **toda consulta devolve um `pandas.DataFrame` já normalizado** —
mesmas colunas, mesma ordem de linhas — para permitir a validação cruzada entre as três
estratégias.

```python
def _normalizar(df, chaves, crescente=True):
    """Ordena e reseta o índice para que as 3 engines saiam comparáveis."""
    return df.sort_values(chaves, ascending=crescente).reset_index(drop=True)
```

#### Q1 — Agregação + Filtro

Total de vendas por categoria, apenas pedidos ≥ R$ 100.
Retorna: `product_category, total_vendas, quantidade_pedidos`

```python
# A) CSV: carrega tudo, depois filtra
def q1_csv():
    df = pd.read_csv(ARQ_CSV, parse_dates=["sale_date"])
    filtrado = df[df["order_value"] >= 100.0]
    return filtrado.groupby("product_category", as_index=False).agg(
        total_vendas=("order_value", "sum"),
        quantidade_pedidos=("order_id", "count"))

# B) Parquet sem otimização: lê TODAS as 10 colunas e TODAS as linhas
def q1_parquet():
    tabela = pq.read_table(ARQ_PARQUET)
    return _agregar_q1(tabela.filter(pc.field("order_value") >= 100.0))

# C) Parquet otimizado: 2 de 10 colunas + filtro no nível do arquivo
def q1_parquet_opt():
    tabela = pq.read_table(ARQ_PARQUET,
        columns=["product_category", "order_value"],      # Column Projection
        filters=[("order_value", ">=", 100.0)])           # Predicate Pushdown
    return _agregar_q1(tabela)
```

#### Q2 — GROUP BY com múltiplas colunas

Vendas por região × método de pagamento × status (45 grupos).
Retorna: `region, payment_method, status, total, count`

```python
def q2_parquet_opt():
    # Q2 não tem WHERE -> não há Predicate Pushdown aplicável.
    # O ganho aqui vem EXCLUSIVAMENTE do Column Projection (4 de 10 colunas).
    return _agregar_q2(pq.read_table(ARQ_PARQUET,
        columns=["region", "payment_method", "status", "order_value"]))
```

#### Q3 — Filtro complexo + Agregação (a consulta mais interessante)

Média, máximo e mínimo do valor por região, **apenas Pix** e **apenas datas após
2024-01-01**.
Retorna: `region, media_valor, max_valor, min_valor`

```python
def q3_parquet_opt():
    # As colunas usadas no filtro NÃO precisam estar na projeção: o pushdown
    # avalia payment_method/sale_date no scan e devolve só region + order_value.
    tabela = pq.read_table(ARQ_PARQUET,
        columns=["region", "order_value"],
        filters=[("payment_method", "==", "Pix"),
                 ("sale_date", ">", datetime(2024, 1, 1))])
    return _agregar_q3(tabela)
```

**Detalhe técnico que vale um slide:** pedimos apenas `region` e `order_value` na projeção,
mas filtramos por `payment_method` e `sale_date`. O leitor usa essas duas colunas
internamente para avaliar o predicado e **as descarta antes de devolver a tabela**. Ou
seja, projeção e pushdown são independentes — não é preciso "pagar" pelas colunas do
`WHERE`.

#### Q4 — Seleção de colunas específicas

Top 10 clientes por gasto total — lendo **apenas as 2 colunas necessárias**.
Retorna: `customer_id, total_gasto`

```python
def q4_parquet_opt():
    # Sem WHERE: puro Column Projection -- 2 de 10 colunas.
    return _top_clientes(pq.read_table(ARQ_PARQUET,
        columns=["customer_id", "order_value"]))
```

**Q2 e Q4 não têm cláusula `WHERE`, então nelas só cabe projeção. Isso é proposital:**
serve para separar experimentalmente o efeito de cada técnica ao longo do relatório.

#### O experimento de isolamento (diferencial do trabalho)

A versão "com otimização" liga projeção e pushdown ao mesmo tempo — o que **impede dizer
qual das duas fez o trabalho**. Para resolver isso, criei duas variantes extras da Q3 que
ligam uma otimização de cada vez:

```python
def q3_apenas_projecao():    # columns= sem filters=
def q3_apenas_pushdown():    # filters= sem columns=
```

E uma função que **comprova a poda** lendo as estatísticas do footer:

```python
def analisar_pruning(coluna="sale_date", corte=DATA_CORTE_Q3):
    """Lê as estatísticas do footer e conta quantos row groups o filtro pode podar.

    Um row group só pode ser descartado sem leitura se seu intervalo [min, max]
    não intersecta o predicado -- aqui, se max(sale_date) <= corte.
    """
    arquivo = pq.ParquetFile(ARQ_PARQUET)
    for i in range(arquivo.num_row_groups):
        estatisticas = arquivo.metadata.row_group(i).column(indice).statistics
        ...
```

Isso transforma a afirmação "o pushdown poda row groups" de teoria em **fato verificado**:
4 de 10 blocos descartados.

### Seção 4 — O runner: a função reutilizável

```python
def executar_consulta(funcao, alvo: str, repeticoes=REPETICOES) -> dict:
    """Função reutilizável: mede tempo (timeit) e pico de memória (psutil).

    Tempo e memória vêm de passadas separadas de propósito:
      - tempo   -> no processo atual, com o cache do SO já aquecido (timeit);
      - memória -> num subprocesso limpo, para o pico ser comparável.
    """
    tempos = medir_tempo(funcao, repeticoes)
    return {"tempo_ms": tempos["mediana"], "tempo_desvio_ms": tempos["desvio"],
            "memoria_mb": medir_memoria(alvo), "resultado": funcao()}
```

Era um requisito do trabalho ("função reutilizável para executar queries"), e as 12
medições passam todas por ela. As consultas ficam registradas em uma estrutura de dados
declarativa, de modo que acrescentar uma Q5 exigiria apenas adicionar uma entrada:

```python
CONSULTAS = [
    {"id": "Q1", "titulo": "Agregação + Filtro", "descricao": "...",
     "impls": {"csv": q1_csv, "parquet": q1_parquet, "parquet_opt": q1_parquet_opt}},
    ...
]
```

#### A validação cruzada — garantia de que a comparação é honesta

```python
def validar(medidas, consulta_id) -> bool:
    """Confere se as 3 implementações produziram exatamente o mesmo resultado."""
```

Compara as três saídas **célula a célula** (`np.allclose` com `rtol=1e-6` para números,
comparação exata para texto) e imprime `validação: OK` a cada execução. Isso responde
antecipadamente à objeção mais óbvia contra qualquer benchmark: *"a versão rápida não está
rápida porque está fazendo menos coisa?"*. Não está — os três caminhos entregam resultados
idênticos. **Sem essa validação, o benchmark não provaria nada.**

### Seção 5 — Geração do relatório

O `RESULTS.md` é **gerado por código**, não escrito à mão: as tabelas, as porcentagens, os
gráficos ASCII e até as frases de análise recebem os números da execução por interpolação.
Isso significa que o relatório nunca fica desatualizado em relação aos dados — rodar de
novo em outra máquina produz um relatório coerente com aquela máquina.

Os gráficos são barras em ASCII (o trabalho restringe as bibliotecas, o que exclui
matplotlib). O gráfico de tempo usa **escala logarítmica**, porque CSV e Parquet diferem em
duas ordens de grandeza e uma escala linear achataria todas as barras de Parquet a um
pixel.

### Seção 6 — `main()` e a interface de linha de comando

```
python script_benchmark.py                 # gera dados (se faltarem) + benchmark + relatório
python script_benchmark.py --apenas-dados  # só gera os arquivos em ./data
python script_benchmark.py --regerar       # força a regeração do dataset
python script_benchmark.py --linhas 2000000 --repeticoes 5
```

---

## 4. Resultados obtidos

Ambiente: Windows 11, Python 3.14.4, 12 CPUs lógicas, 15,7 GB RAM, SSD.
Bibliotecas: pandas 3.0.2, pyarrow 25.0.1, numpy 2.4.4, psutil 7.2.2.
Dataset: 1.000.000 registros, 10 colunas, 10 row groups.

### 4.1 Tempo de execução (mediana de 9 execuções, em ms)

| Query | CSV | Parquet (sem opt) | Parquet (com opt) | Melhoria % | Speedup |
|---|---:|---:|---:|---:|---:|
| **Q1** | 2.666,4 | 91,0 | 30,9 | **98,8%** | **86,2x** |
| **Q2** | 2.798,0 | 122,1 | 93,1 | **96,7%** | **30,1x** |
| **Q3** | 2.682,7 | 98,5 | 40,7 | **98,5%** | **65,9x** |
| **Q4** | 2.647,3 | 109,9 | 48,3 | **98,2%** | **54,8x** |

### 4.2 Pico de memória (em MB)

| Query | CSV | Parquet (sem opt) | Parquet (com opt) | Redução vs CSV |
|---|---:|---:|---:|---:|
| **Q1** | 178,1 | 181,8 | 66,4 | **62,7%** |
| **Q2** | 194,6 | 177,7 | 101,5 | **47,8%** |
| **Q3** | 178,3 | 179,4 | 70,1 | **60,7%** |
| **Q4** | 177,8 | 198,3 | 81,5 | **54,1%** |

### 4.3 Tamanho em disco

| Formato | Tamanho | Relativo |
|---|---:|---:|
| CSV (texto puro) | 76,5 MB | 100% |
| Parquet (snappy) | 22,8 MB | 29,8% |

**Redução de 70,2%** para exatamente o mesmo conteúdo.

### 4.4 Experimento de isolamento (Q3, mediana de 21 execuções)

| Otimização ativa | Tempo | Desvio-padrão | Memória | Ganho de tempo | Ganho de memória |
|---|---:|---:|---:|---:|---:|
| Nenhuma (lê tudo) | 100,5 ms | ± 12,2 | 173,9 MB | — | — |
| Só Column Projection | 47,3 ms | ± 7,7 | 95,9 MB | **2,12x** | **1,81x** |
| Só Predicate Pushdown | 73,4 ms | ± 7,1 | 124,1 MB | **1,37x** | **1,40x** |
| **As duas juntas** | **39,4 ms** | ± 3,8 | **68,5 MB** | **2,55x** | **2,54x** |

Poda verificada no footer: **4 de 10 row groups descartados** pelo filtro de data.

---

## 5. As três descobertas que diferenciam o trabalho

Estas são as conclusões que vão além do esperado e devem ganhar destaque na apresentação.
Qualquer um consegue mostrar que Parquet é mais rápido; o valor aqui está em explicar o
mecanismo e em um achado que **contraria a expectativa**.

### Descoberta 1 — Parquet, por si só, NÃO economiza memória

Olhe a coluna do meio da tabela 4.2: ler o Parquet **inteiro** consome praticamente a mesma
RAM que o CSV — 178 vs 182 MB na Q1 — e na **Q4 consome mais** (198 vs 178 MB).

**Por quê:** se todas as 10 colunas e 1 milhão de linhas vão para a memória, um `Table` do
Arrow ocupa aproximadamente o mesmo que um `DataFrame` do pandas. **A compressão existe no
arquivo, não na memória.**

> **Frase-chave para o slide:** *Parquet não economiza RAM por ser Parquet — economiza
> porque permite não ler o que não será usado. Quem migra para Parquet e continua lendo
> tudo leva só metade do benefício.*

Sem `columns=`/`filters=`, o benefício é **só de tempo** (desaparece o parse de texto); a
economia de memória vem inteira das otimizações de leitura.

### Descoberta 2 — A contribuição de cada otimização, medida separadamente

O experimento de isolamento (tabela 4.4) mostra que as duas técnicas atacam **eixos
diferentes do mesmo scan**:

- **Column Projection elimina COLUNAS** → 2,12x em tempo, 1,81x em memória
- **Predicate Pushdown elimina LINHAS** → 1,37x em tempo, 1,40x em memória
- **Juntas: 2,55x em tempo e 2,54x em memória** — melhor que qualquer uma isolada, porque
  os cortes são ortogonais

### Descoberta 3 — O pushdown rende menos tempo do que se espera, e sei explicar por quê

Contra a intuição de que "pushdown = tempo", a projeção foi a técnica dominante **nos dois
eixos** em todas as execuções do benchmark. Três motivos se somam:

1. **A poda só elimina 4 dos 10 row groups.** O predicado de data poda bem (os dados estão
   ordenados por `sale_date`), mas o filtro de `payment_method` **não poda nada**: todo row
   group contém os três métodos de pagamento, então seus `min`/`max` sempre intersectam o
   predicado.
2. **Nos row groups que sobram, sem projeção ainda é preciso decodificar as 10 colunas.** O
   filtro corta linhas, mas o custo por linha continua alto.
3. **`filters=` troca o leitor direto pelo scanner do `pyarrow.dataset`**, que avalia a
   expressão lote a lote — um overhead que só se paga quando há muito dado a descartar.

> **A lição prática:** o Predicate Pushdown entrega tempo **na proporção do que consegue
> não ler**. Com ~35% das linhas sobrevivendo ao filtro e 4/10 row groups podados, ainda
> sobra trabalho demais para o ganho ser dramático. Ele se torna decisivo quando o layout
> físico colabora — particionamento por data, ordenação pela coluna de filtro, predicados
> muito seletivos. A projeção, em contraste, entrega ganho **garantido** sempre que a
> consulta não usa todas as colunas, sem depender de como o arquivo foi organizado.

Essa descoberta é o que fundamenta a recomendação de particionar o Bronze por data: partição
é pushdown no nível de **diretório**, onde arquivos fora da janela nem chegam a ser abertos.

---

## 6. Recomendações arquiteturais

### Qual formato escolher

**Parquet, para toda a camada analítica.** Em média 59x mais rápido que CSV nas 4 consultas,
com 70% menos disco e ~56% menos memória *quando lido com projeção e filtros*. As três
implementações foram validadas célula a célula e produzem resultados idênticos — o ganho
não custa precisão.

**Quando manter CSV ainda faz sentido:**
- intercâmbio com sistemas ou pessoas que só leem texto (export de relatório, planilha para
  a área de negócio);
- arquivos pequenos (< ~10 MB), onde a diferença é irrelevante;
- ingestão bruta de terceiros, que chega em CSV e deve ser preservada como recebida por
  auditoria.

### Impacto nas camadas do Data Lake

| Camada | Formato recomendado | Racional |
|---|---|---|
| **Raw / Landing** | arquivo original (CSV/JSON) + Parquet espelhado | preserva o dado como recebido (auditoria e reprocessamento) e já materializa uma cópia consultável |
| **Bronze** | **Parquet** (snappy), particionado por data | primeiro ponto em que o dado ganha schema e tipos; tudo a jusante lê daqui |
| **Silver** | **Parquet** limpo/deduplicado, particionado | consultas exploratórias já pagam o custo reduzido |
| **Gold** | **Parquet** agregado (ou tabela do DW) | resultados prontos para BI |

**Cinco diretrizes que o benchmark sustenta:**

1. **Converta para Parquet no Bronze, não depois.** Cada consulta a jusante que ainda toque
   CSV paga de novo o custo de parse. A conversão é um custo único; a leitura acontece
   milhares de vezes.
2. **Particione por `sale_date`** (`ano=2024/mes=03/`). É pushdown no nível de diretório — o
   degrau seguinte ao que foi medido aqui, e o que faltava para o pushdown render em tempo.
3. **Cuide do tamanho do row group.** Grandes demais matam o pushdown (não há o que podar);
   pequenos demais inflam o footer e geram I/O fragmentado. ~128 MB descomprimidos é o alvo.
4. **Ordene (ou faça cluster) pelas colunas de filtro mais comuns** antes de gravar.
   Estatísticas min/max só podam se os dados estiverem correlacionados com a ordem física —
   foi o que tornou o pushdown por data eficaz, e a falta disso é o que torna o filtro por
   `payment_method` inútil para poda.
5. **Evite arquivos pequenos.** Milhares de Parquets de poucos KB custam mais em abertura de
   footer do que economizam; consolide (*compaction*) para 128-512 MB.

### Equivalente declarativo em DuckDB

O DuckDB aplica projeção e pushdown **automaticamente** a partir do SQL — lê o footer, poda
row groups pelas estatísticas e materializa só as colunas do `SELECT`/`WHERE`. O mesmo que
foi feito à mão com PyArrow, só que declarativo:

```sql
-- Q3: projection (2 de 10 colunas) + pushdown (payment_method e sale_date)
SELECT region,
       AVG(order_value) AS media_valor,
       MAX(order_value) AS max_valor,
       MIN(order_value) AS min_valor
FROM   read_parquet('data/dados_vendas.parquet')
WHERE  payment_method = 'Pix'
  AND  sale_date > DATE '2024-01-01'
GROUP  BY region;

-- Conferindo que a poda aconteceu: o plano mostra 'Filters: ...' no
-- PARQUET_SCAN e a contagem de row groups efetivamente lidos.
EXPLAIN ANALYZE SELECT region, AVG(order_value) FROM ...;

-- Conversão CSV -> Parquet particionado (o passo Raw -> Bronze)
COPY (SELECT * FROM read_csv_auto('data/dados_vendas.csv'))
TO 'lake/bronze/vendas'
(FORMAT PARQUET, COMPRESSION SNAPPY,
 PARTITION_BY (year(sale_date), month(sale_date)),
 ROW_GROUP_SIZE 100000);
```

---

## 7. Transparência metodológica (e por que isso conta a favor)

Dois pontos que o relatório declara abertamente, em vez de esconder:

**Variabilidade das medições.** As leituras Parquet duram dezenas de ms e têm
desvio-padrão de até 45% da mediana. O relatório publica o desvio ao lado de cada medição
de isolamento e avisa: *diferenças menores que ~20% entre duas variantes de Parquet não
devem ser lidas como significativas*. A diferença entre CSV e Parquet, de uma a duas
ordens de grandeza, está muito acima desse ruído — é ela que sustenta a conclusão.

**Bibliotecas.** Além das exigidas (pandas, pyarrow, numpy, psutil, time/timeit), usei
apenas módulos da **biblioteca padrão** do Python: `subprocess` (medir memória em processo
limpo), `threading` (amostrar o pico), `statistics`, `gc`, `argparse`, `pathlib`. Nenhuma
dependência externa adicional. Os gráficos são ASCII porque a restrição de bibliotecas
exclui matplotlib.

---

## 8. Sugestão de roteiro de slides

> Para a IA que vai montar a apresentação: esta é uma sugestão de estrutura, não uma
> camisa de força. Priorize a seção 5 (as descobertas) — é o conteúdo de maior valor.
> Evite encher slides de texto: use as tabelas de números e deixe a explicação para a
> fala. Sugestão de tom: técnico, direto, sem jargão de marketing.

| # | Slide | Conteúdo central |
|---|---|---|
| 1 | **Capa** | Título, autor, Trilha B — Otimização de Consultas Analíticas |
| 2 | **O problema** | E-commerce, 1M de registros em CSV, relatórios lentos. A pergunta do trabalho |
| 3 | **CSV vs Parquet: a diferença estrutural** | Linha vs coluna; sem tipos vs com schema; o footer com min/max. Diagrama simples ajuda muito aqui |
| 4 | **As duas otimizações** | Column Projection (corta colunas) e Predicate Pushdown (corta linhas), com a linha de código de cada |
| 5 | **Metodologia** | 4 consultas × 3 estratégias; mediana de 9 execuções com timeit; pico de memória com psutil em subprocesso limpo; validação cruzada célula a célula |
| 6 | **O dataset** | 1M registros, 10 colunas, gerado vetorizado com numpy; lognormal para valores; **ordenado por data** (e por que isso importa) |
| 7 | **Resultados: tempo** | Tabela 4.1. Destaque: 30x a 86x |
| 8 | **Resultados: memória** | Tabela 4.2. Preparar o terreno para a Descoberta 1 |
| 9 | **Descoberta 1** | Parquet sozinho não economiza RAM — a compressão está no arquivo, não na memória |
| 10 | **Descoberta 2** | Tabela de isolamento: projeção 2,12x, pushdown 1,37x, juntas 2,55x. Eixos ortogonais |
| 11 | **Descoberta 3** | O pushdown rende menos do que se espera, e os três motivos. A lição sobre layout físico |
| 12 | **Disco e custo** | 76,5 → 22,8 MB (−70%). Impacto em serviços cobrados por bytes escaneados |
| 13 | **Recomendação arquitetural** | Tabela de camadas Raw/Bronze/Silver/Gold + as diretrizes principais |
| 14 | **Conclusão** | Parquet para toda a camada analítica, mas: *trocar o formato é metade do trabalho* |

---

## 9. Perguntas prováveis da banca e respostas

**"Por que o CSV é tão lento? É o disco?"**
Não é I/O, é **parse**. Converter texto em tipo nas 10 colunas, sendo que 8 não são usadas
pela consulta. E `parse_dates` em 1 milhão de timestamps é o item mais caro da carga. Além
disso, não existe ler "meio CSV": sem índice nem metadados, qualquer byte pode conter a
linha que interessa.

**"O ganho não vem só do cache do sistema operacional?"**
As três estratégias leem do mesmo cache aquecido, nas mesmas condições, com o mesmo número
de repetições. A comparação é simétrica.

**"A versão rápida não está rápida porque faz menos coisa?"**
Não — e isso é verificado automaticamente. A função `validar()` compara as três saídas
célula a célula a cada execução e imprime `validação: OK`. Resultados idênticos.

**"E se o dataset não couber na RAM?"**
A vantagem do Parquet cresce: projeção e pushdown reduzem o que precisa ser materializado,
e o formato suporta leitura em lotes (`iter_batches`). O CSV precisaria de chunking manual
e ainda pagaria o parse de tudo.

**"Por que a Q2 ganhou menos (30x)?"**
É a única que agrupa por 3 colunas de texto e devolve 45 grupos — a agregação pesa mais no
total. E, sem `WHERE`, só há projeção de 4 de 10 colunas, sem pushdown.

**"Por que medir memória em subprocesso em vez de usar memory_profiler?"**
Restrição de bibliotecas do trabalho. E `psutil` com subprocesso limpo dá o número mais
fiel de qualquer forma, porque usa `peak_wset` — o pico real registrado pelo próprio
sistema operacional.

**"Por que mediana e não média ou mínimo?"**
Mínimo é instável nesse regime de ruído e chegou a inverter a ordem entre variantes entre
execuções. Média é contaminada por outliers de escalonamento do SO. Mediana é robusta nas
duas pontas e representa o tempo típico.

**"Esses dados são sintéticos — a conclusão vale para dados reais?"**
As características que determinam o resultado foram modeladas de propósito: cardinalidade
realista nas colunas categóricas, distribuição assimétrica de valores e ordenação
cronológica (como em um histórico acumulado por ETL). O mecanismo medido — projeção e poda
por estatísticas — depende dessas propriedades estruturais, não de os dados serem reais.
