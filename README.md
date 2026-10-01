# Otimização de Consultas Analíticas — CSV vs Parquet

Benchmark que compara o custo de executar as mesmas consultas analíticas sobre
**1.000.000 de registros** de vendas de e-commerce em três estratégias:

| # | Estratégia | Como lê os dados |
|---|---|---|
| A | **CSV + pandas** | `pd.read_csv()` — arquivo inteiro, todas as colunas |
| B | **Parquet + PyArrow** | `pq.read_table()` — tabela inteira, todas as colunas |
| C | **Parquet + PyArrow otimizado** | `columns=[...]` (Column Projection) + `filters=[...]` (Predicate Pushdown) |

O resultado completo, com tabelas, gráficos e análise, fica em
[results/RESULTS.md](results/RESULTS.md).

---

## Como rodar

### 1. Requisitos

Python 3.9 ou superior.

```bash
pip install -r requirements.txt
```

### 2. Executar tudo (dados + benchmark + relatório)

```bash
python script_benchmark.py
```

Na primeira execução o script gera o dataset (`data/dados_vendas.csv` e
`data/dados_vendas.parquet`) e depois roda o benchmark. Nas execuções seguintes
ele reaproveita os arquivos existentes.

Tempo total aproximado: **2 a 4 minutos** (a maior parte é a geração do CSV e as
leituras repetidas dele).

### 3. Opções

```bash
python script_benchmark.py --regerar            # recria o dataset do zero
python script_benchmark.py --apenas-dados       # só gera os arquivos, sem benchmark
python script_benchmark.py --linhas 5000000     # dataset maior
python script_benchmark.py --repeticoes 10      # mais repetições por medição
```

### 4. Saída

```
data/dados_vendas.csv        ~76 MB
data/dados_vendas.parquet    ~23 MB
results/RESULTS.md           relatório completo
```

O relatório é **regravado a cada execução** com os números da máquina em que rodou.

---

## Estrutura do projeto

```
.
├── data/                         # NÃO versionado -- ver nota abaixo
│   ├── dados_vendas.csv          # 1M registros, 10 colunas (gerado)
│   └── dados_vendas.parquet      # mesmo conteúdo, snappy, 10 row groups (gerado)
├── results/
│   └── RESULTS.md                # relatório com tabelas, gráficos e análise
├── script_benchmark.py           # geração + consultas + benchmark + relatório
├── BRIEFING_APRESENTACAO.md      # material de apoio para apresentar o trabalho
├── requirements.txt
└── README.md
```

> **A pasta `data/` não está no repositório** (são ~99 MB de artefato derivado). Ela é
> criada na primeira execução. Como a seed é fixa (`SEED = 42`), o dataset regenerado é
> **byte-idêntico** ao usado no relatório — basta rodar `python script_benchmark.py`
> (ou `--apenas-dados` para só gerar os arquivos). Os resultados medidos já estão
> versionados em [results/RESULTS.md](results/RESULTS.md), legíveis sem rodar nada.

O `script_benchmark.py` está dividido em seções numeradas:

| Seção | Conteúdo |
|---|---|
| 1 | Instrumentação — medição de tempo (`timeit`) e de memória (`psutil`) |
| 2 | Geração do dataset sintético (vetorizada com `numpy`) |
| 3 | As 4 consultas, cada uma em 3 implementações + experimento de isolamento |
| 4 | Execução do benchmark e validação cruzada dos resultados |
| 5 | Geração do `RESULTS.md` |
| 6 | CLI (`main`) |

---

## O dataset

1.000.000 de linhas geradas com `numpy` (seed fixa `42`, então o dataset é
reprodutível), sem nenhum loop em Python:

| Coluna | Tipo | Domínio |
|---|---|---|
| `order_id` | int64 | 1 … 1.000.000 |
| `customer_id` | int32 | 1 … 50.000 |
| `product_id` | int32 | 1 … 5.000 |
| `product_category` | string | 8 categorias (Eletronicos, Roupas, Alimentos, …) |
| `sale_date` | datetime | 2023-01-01 a 2024-12-31 |
| `order_value` | float64 | lognormal (cauda longa), R$ 5 a R$ 25.000 |
| `quantity` | int16 | 1 … 10 |
| `region` | string | Norte, Nordeste, Centro-Oeste, Sudeste, Sul |
| `payment_method` | string | Cartao, Boleto, Pix |
| `status` | string | Concluido, Cancelado, Pendente |

Duas decisões de geração afetam diretamente o benchmark e estão documentadas no
código:

- **os dados são gravados ordenados por `sale_date`** — é como um histórico de
  vendas realmente se acumula, e é o que permite às estatísticas min/max de cada
  row group podar blocos no filtro por data;
- **`row_group_size=100_000`** (10 row groups) — com um único row group de 1M de
  linhas não haveria nada para o Predicate Pushdown podar.

---

## As consultas

| Query | Tipo | O que retorna |
|---|---|---|
| **Q1** | Agregação + filtro | Total de vendas por categoria, apenas vendas ≥ R$ 100 → `product_category, total_vendas, quantidade_pedidos` |
| **Q2** | GROUP BY múltiplo | Vendas por região × método de pagamento × status → `region, payment_method, status, total, count` |
| **Q3** | Filtro complexo + agregação | Média/máx/mín por região, apenas Pix e `sale_date > 2024-01-01` → `region, media_valor, max_valor, min_valor` |
| **Q4** | Projeção de colunas | Top 10 clientes por gasto → `customer_id, total_gasto` |

Q2 e Q4 não têm `WHERE`: nelas a única otimização aplicável é o Column
Projection, o que serve justamente para separar o efeito de cada técnica.

---

## Como as medições são feitas

**Tempo** — `timeit.repeat(..., number=1)` com 5 repetições, reportando a
melhor. Cada medição cobre o **pipeline inteiro**: abrir o arquivo, ler, fazer
parse e agregar — que é o tempo que o analista realmente espera. O cache de
arquivos do SO está aquecido para os dois formatos, então a comparação é justa.

**Memória** — cada consulta é medida em um **subprocesso recém-criado**. Isso não
é preciosismo: o RSS de um processo não encolhe depois do primeiro pico, então
medir várias consultas no mesmo processo faria a segunda em diante aparecer como
~0 MB. No Windows o valor vem do `peak_wset` (pico exato informado pelo SO); nas
demais plataformas, de uma thread que amostra o RSS a cada 2 ms.

**Correção** — a cada execução as 3 implementações de cada consulta são
comparadas célula a célula (`validar()`). Um benchmark em que as versões rápidas
respondem outra coisa não mede nada; o console imprime `validacao cruzada: OK`
quando os três caminhos concordam.

**Atribuição** — como a versão otimizada liga projeção e pushdown ao mesmo tempo,
a Q3 é medida também com **uma otimização de cada vez**, para saber quanto cada
técnica contribui de fato. A tabela está na seção 3.3 do relatório.

---

## Resumo dos resultados

Medido em Windows 11, Python 3.14, 12 CPUs lógicas, SSD:

- Parquet otimizado ficou **de uma a duas ordens de grandeza mais rápido** que CSV
  nas 4 consultas (dezenas a mais de 100x, variando com a consulta e a máquina —
  as leituras Parquet duram dezenas de ms e oscilam bastante entre execuções);
- o arquivo Parquet ocupa **~70% menos disco** que o CSV com o mesmo conteúdo;
- a economia de memória vem **das otimizações de leitura, não do formato**: ler o
  Parquet inteiro consome praticamente a mesma RAM que o CSV — é o `columns=` que
  derruba o pico para menos da metade.

Os números exatos da sua máquina ficam em [results/RESULTS.md](results/RESULTS.md)
após rodar o script.

---

## Bibliotecas

`pandas`, `pyarrow`, `numpy`, `psutil` e `timeit`/`time` — conforme a restrição do
projeto. Os snippets DuckDB da seção 5 do relatório são ilustrativos e não
precisam ser instalados para rodar o benchmark.
