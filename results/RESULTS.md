# Benchmark: CSV vs Parquet em Consultas Analíticas

> Gerado automaticamente por `script_benchmark.py` em 22/09/2026 11:42.

## Ambiente e dataset

| Item | Valor |
|---|---|
| Registros | 1.000.000 |
| Colunas | 10 |
| Arquivo CSV | 76.5 MB |
| Arquivo Parquet (snappy) | 22.8 MB |
| Redução em disco | **70.2%** |
| Row groups no Parquet | 10 (100.000 linhas cada) |
| Repetições por medição (timeit) | 9 (reportada a **mediana**) |
| Bibliotecas | pandas 3.0.2, pyarrow 25.0.1, numpy 2.4.4, psutil 7.2.2 |
| CPUs lógicas / RAM | 12 / 15.7 GB |

As três estratégias comparadas medem sempre a **consulta inteira**, do arquivo em disco até o resultado agregado (I/O + parse + agregação) — que é o tempo que o analista de fato espera.

## As consultas

| Query | Tipo | O que calcula | Retorna | Otimizações aplicáveis |
|---|---|---|---|---|
| **Q1** | Agregação + Filtro | Total de vendas por categoria, apenas vendas >= R$ 100 | `product_category, total_vendas, quantidade_pedidos` | Projeção 2/10 colunas + pushdown de `order_value` |
| **Q2** | GROUP BY múltiplas colunas | Vendas por região, método de pagamento e status | `region, payment_method, status, total, count` | Projeção 4/10 colunas (sem WHERE: não há pushdown) |
| **Q3** | Filtro complexo + Agregação | Média/máx/mín por região, apenas Pix e sale_date > 2024-01-01 | `region, media_valor, max_valor, min_valor` | Projeção 2/10 colunas + pushdown de `payment_method` e `sale_date` |
| **Q4** | Seleção de colunas específicas | Top 10 clientes por gasto total | `customer_id, total_gasto` | Projeção 2/10 colunas (sem WHERE: não há pushdown) |

Q2 e Q4 não têm cláusula `WHERE` — nelas a única otimização aplicável é o Column Projection. Isso é proposital: serve para separar o efeito de cada técnica ao longo do relatório.

## 1. Desempenho (tempo em ms)

| Query | CSV | Parquet (sem opt) | Parquet (com opt) | Melhoria % | Speedup |
|---|---:|---:|---:|---:|---:|
| **Q1** | 2,666.4 | 91.0 | 30.9 | **98.8%** | 86.2x |
| **Q2** | 2,798.0 | 122.1 | 93.1 | **96.7%** | 30.1x |
| **Q3** | 2,682.7 | 98.5 | 40.7 | **98.5%** | 65.9x |
| **Q4** | 2,647.3 | 109.9 | 48.3 | **98.2%** | 54.8x |

> *Melhoria %* e *Speedup* comparam **CSV vs Parquet otimizado** (a coluna que importa para a decisão de migrar).

Cada célula é a **mediana de 9 execuções**. O desvio-padrão chegou a 408% da mediana no caso mais instável (Q1 / Parquet (sem opt)): leituras Parquet duram dezenas de ms e sofrem influência de alocação, GC e escalonamento do SO. Por isso a mediana, e não o mínimo — diferenças menores que ~20% entre duas variantes de Parquet não devem ser lidas como significativas. Já a diferença entre CSV e Parquet, de uma a duas ordens de grandeza, está muito acima desse ruído.

```text
TEMPO POR CONSULTA (ms) - menor e melhor - ESCALA LOGARITMICA

Q1
  CSV (pandas)        |##################################|    2666.4 ms
  Parquet (sem opt)   |###################               |      91.0 ms
  Parquet (com opt)   |###############                   |      30.9 ms

Q2
  CSV (pandas)        |##################################|    2798.0 ms
  Parquet (sem opt)   |#####################             |     122.1 ms
  Parquet (com opt)   |###################               |      93.1 ms

Q3
  CSV (pandas)        |##################################|    2682.7 ms
  Parquet (sem opt)   |####################              |      98.5 ms
  Parquet (com opt)   |################                  |      40.7 ms

Q4
  CSV (pandas)        |##################################|    2647.3 ms
  Parquet (sem opt)   |####################              |     109.9 ms
  Parquet (com opt)   |#################                 |      48.3 ms
```

## 2. Consumo de memória (pico em MB)

| Query | CSV | Parquet (sem opt) | Parquet (com opt) | Redução vs CSV |
|---|---:|---:|---:|---:|
| **Q1** | 178.1 | 181.8 | 66.4 | **62.7%** |
| **Q2** | 194.6 | 177.7 | 101.5 | **47.8%** |
| **Q3** | 178.3 | 179.4 | 70.1 | **60.7%** |
| **Q4** | 177.8 | 198.3 | 81.5 | **54.1%** |

**Metodologia:** cada consulta é medida em um **subprocesso recém-criado**, porque o RSS de um processo não encolhe depois do primeiro pico — medir várias consultas no mesmo processo faria a segunda em diante aparecer como ~0 MB. O valor é o pico de working set acima da linha de base do interpretador já com as bibliotecas importadas (`psutil.Process().memory_info()`: `peak_wset` no Windows, ou amostragem a cada 2 ms em uma thread dedicada nas demais plataformas).

```text
PICO DE MEMORIA (MB) - menor e melhor

Q1
  CSV (pandas)        |###############################   |     178.1 MB
  Parquet (sem opt)   |###############################   |     181.8 MB
  Parquet (com opt)   |###########                       |      66.4 MB

Q2
  CSV (pandas)        |################################# |     194.6 MB
  Parquet (sem opt)   |##############################    |     177.7 MB
  Parquet (com opt)   |#################                 |     101.5 MB

Q3
  CSV (pandas)        |###############################   |     178.3 MB
  Parquet (sem opt)   |###############################   |     179.4 MB
  Parquet (com opt)   |############                      |      70.1 MB

Q4
  CSV (pandas)        |##############################    |     177.8 MB
  Parquet (sem opt)   |##################################|     198.3 MB
  Parquet (com opt)   |##############                    |      81.5 MB
```

**O achado mais importante desta tabela está na coluna do meio:** ler o Parquet *inteiro* custa praticamente a mesma memória que o CSV (184 MB contra 182 MB, em média) — em algumas consultas chega a custar mais. E faz sentido: se todas as 10 colunas e 1M de linhas vão para a RAM, um `Table` do Arrow ocupa o mesmo que um `DataFrame` do pandas; a compressão existe **no arquivo**, não na memória.

**Parquet não economiza RAM por ser Parquet — economiza porque permite não ler o que não será usado.** Sem `columns=`/`filters=`, o benefício é só de tempo (não há parse de texto); a economia de memória vem inteira das otimizações da coluna da direita.

## 3. Explicação técnica

### 3.1 Por que o CSV é lento antes mesmo da agregação

CSV é um formato **orientado a linha e sem tipos**. Para responder "soma de `order_value` por categoria", o pandas precisa:

1. ler os ~76 MB do arquivo inteiro do disco;
2. fazer *parse* de texto para tipo em **todas as 10 colunas**, inclusive as 8 que a consulta não usa (converter `"1234.56"` em float é caro, e `parse_dates` em 1M de timestamps é o item mais pesado da carga);
3. materializar o DataFrame completo na RAM;
4. só então filtrar e agregar.

Os passos 1 a 3 dominam o tempo total. **Não existe ler "meio CSV"**: sem índice e sem metadados, qualquer byte do arquivo pode conter a linha que interessa.

### 3.2 Como o Column Projection ajudou (redução de memória)

Parquet armazena os dados **por coluna**: cada coluna é um bloco contíguo e independente dentro do arquivo, e o rodapé (*footer*) diz exatamente em que offset cada bloco começa. Pedir `columns=[...]` faz o leitor posicionar o ponteiro apenas nos blocos pedidos — as demais colunas **nunca são lidas do disco nem descompactadas**.

O efeito acompanha a fração de colunas lidas, ainda que não seja exatamente linear (colunas de texto pesam muito mais que inteiros, e parte do pico é overhead fixo do leitor). Em Q4, que precisa apenas de `customer_id` e `order_value` — 2 de 10 colunas —, o pico de memória caiu de 198.3 MB (tabela inteira) para 81.5 MB (~2.4x menos), contra 177.8 MB do CSV.

Esse é também o motivo de a projeção ser a única otimização possível em Q2 e Q4: consultas sem `WHERE` não têm predicado para empurrar.

A projeção melhora os dois indicadores (ver a tabela de isolamento em 3.3), mas é **na memória que ela é decisiva**: deixar de decodificar uma coluna poupa alguns milissegundos de CPU e, ao mesmo tempo, dezenas de MB que nunca precisam existir como array Arrow. Em produção é justamente o pico de memória que derruba o job (OOM) quando o volume cresce — o tempo apenas aumenta, a memória mata o processo.

### 3.3 Como o Predicate Pushdown ajudou (redução de tempo)

`filters=[...]` empurra o predicado para **dentro** do leitor Parquet, onde ele atua em duas camadas:

- **Poda de row groups (economiza I/O):** o footer guarda `min`/`max` de cada coluna em cada um dos 10 row groups. Se o intervalo do row group não intersecta o filtro, o bloco inteiro é pulado sem ser lido nem descompactado. Como o dataset foi gravado **ordenado por `sale_date`**, o predicado `sale_date > 2024-01-01` da Q3 elimina **4 dos 10 row groups** apenas olhando as estatísticas — conferido lendo o footer com `ParquetFile.metadata` (função `analisar_pruning()`).
- **Filtro no scan (economiza RAM e CPU):** nos row groups que sobraram, as linhas são avaliadas durante a leitura, então o `Table` já nasce filtrado. Sem pushdown, o PyArrow materializa 1M de linhas e só depois joga fora o que não passa.

**Atribuindo o ganho corretamente.** A versão "com opt" liga as duas otimizações ao mesmo tempo, o que não permite dizer qual delas fez o trabalho. Rodamos então a Q3 com uma de cada vez (21 repetições por variante, mediana):

| Otimização ativa em Q3 | Tempo (ms) | Desvio-padrão | Pico de memória (MB) |
|---|---:|---:|---:|
| Nenhuma (lê tudo) | 100.5 | ± 12.2 | 173.9 |
| Só Column Projection | 47.3 | ± 7.7 | 95.9 |
| Só Predicate Pushdown | 73.4 | ± 7.1 | 124.1 |
| Projection + Pushdown | 39.4 | ± 3.8 | 68.5 |

> A linha "Nenhuma" mede a mesma consulta da coluna *Parquet (sem opt)* da tabela 1, mas em uma bateria separada de 21 execuções — por isso os dois valores não coincidem exatamente. Compare as variantes **dentro** desta tabela, não entre tabelas; os desvios-padrão da terceira coluna dão a escala do ruído.

Sozinhas, em relação à leitura sem otimização: a **projeção** rende 2.12x em tempo e 1.81x em memória; o **pushdown** rende 1.37x em tempo e 1.40x em memória. Juntas chegam a 39.4 ms e 68.5 MB.

Duas leituras saem daí — e a segunda contraria a expectativa de que "pushdown = tempo":

**1. As duas otimizações cortam memória, por mecanismos diferentes.** A projeção (1.81x) elimina **colunas**; o pushdown (1.40x) elimina **linhas**. Como atacam eixos distintos do mesmo scan, combiná-las chega a 68.5 MB — melhor que qualquer uma isolada.

**2. Em tempo, a projeção lidera** (2.12x, contra 1.37x do pushdown) — e essa ordem se repetiu em todas as execuções deste benchmark, embora as magnitudes oscilem. O teto do pushdown aqui é estrutural, e entender por quê é o que define quando cada técnica compensa:

- a poda por estatísticas elimina apenas 4 dos 10 row groups. O predicado de data poda bem (os dados estão ordenados por `sale_date`), mas o de `payment_method` **não poda nada**: todo row group contém os três métodos de pagamento, então `min`/`max` sempre intersectam o filtro;
- nos row groups que sobram, sem projeção ainda é preciso decodificar **as 10 colunas**. O filtro corta linhas, mas o custo por linha continua alto;
- `filters=` troca o leitor direto pelo scanner do `pyarrow.dataset`, que avalia a expressão lote a lote — overhead que só se paga quando há muito dado a descartar.

**A lição prática:** o Predicate Pushdown entrega tempo na proporção do que ele consegue *não ler*. Com ~35% das linhas sobrevivendo ao filtro e 4/10 row groups podados, ainda sobra trabalho demais para que o ganho seja dramático. Ele se torna decisivo quando o layout físico colabora — particionamento por data, ordenação pela coluna de filtro, predicados muito seletivos — e é exatamente por isso que a seção 4.3 recomenda particionar o Bronze por `sale_date`. A projeção, em contraste, entrega ganho garantido sempre que a consulta não usa todas as colunas, sem depender de como o arquivo foi organizado.

> **Detalhe de implementação que vale registrar:** as colunas usadas no filtro não precisam estar na projeção. Em Q3 pedimos `columns=['region','order_value']` e filtramos por `payment_method`/`sale_date` — o leitor usa essas colunas internamente e as descarta antes de devolver a tabela.

### 3.4 Qual consulta mais se beneficiou

**Q1 (Agregação + Filtro)**, com **86.2x** de speedup sobre o CSV (2,666.4 ms → 30.9 ms).

O padrão geral: **quanto menos dado a consulta realmente precisa, maior o ganho**. Consultas que leem poucas colunas e filtram muitas linhas (Q1, Q3) combinam as duas otimizações e chegam a ordens de grandeza; consultas sem filtro (Q2, Q4) ganham "apenas" pela projeção e pela ausência de parse de texto — o que já é expressivo. Nenhuma consulta ficou mais lenta em Parquet.

## 4. Recomendações arquiteturais

### 4.1 Tamanho em disco

| Formato | Tamanho | Relativo | Observação |
|---|---:|---:|---|
| CSV (texto puro) | 76.5 MB | 100% | sem tipos, sem compressão |
| Parquet (snappy) | 22.8 MB | 29.8% | colunar, dicionário + RLE + snappy |

Redução de **70.2%** para o mesmo conteúdo. A compressão colunar funciona tão bem porque valores semelhantes ficam vizinhos: colunas de baixa cardinalidade (`region`, `status`, `payment_method`) viram dicionários de 3 a 8 entradas, e números são guardados em binário em vez de texto. Em nuvem isso vira dinheiro direto — menos GB armazenados e, sobretudo, menos GB *escaneados* por query em serviços cobrados por leitura (Athena, BigQuery).

### 4.2 Conclusão: qual formato escolher

**Parquet, para toda a camada analítica.** Em média **59.2x** mais rápido que CSV nas 4 consultas, com 70% menos disco e 56% menos memória *quando lido com projeção e filtros*. As três implementações foram validadas célula a célula e produzem resultados idênticos, então o ganho não custa precisão.

A ressalva em itálico é a lição prática deste benchmark: **trocar o formato é metade do trabalho**. Um `pq.read_table()` sem argumentos já resolve o tempo (o parse de texto desaparece), mas mantém o mesmo consumo de RAM do CSV. Quem migra para Parquet e continua lendo tudo leva só metade do benefício.

Quando **manter CSV** ainda faz sentido:

- intercâmbio com sistemas ou pessoas que só leem texto (export de relatório, planilha para a área de negócio);
- arquivos pequenos (< ~10 MB), onde a diferença é irrelevante;
- ingestão bruta vinda de terceiros, que chega em CSV e deve ser preservada como recebida, por auditoria.

### 4.3 Impacto na camada Raw/Bronze do Data Lake

| Camada | Formato recomendado | Racional |
|---|---|---|
| **Raw / Landing** | arquivo original (CSV/JSON) + Parquet espelhado | preserva o dado como recebido (auditoria e reprocessamento) e já materializa uma cópia consultável |
| **Bronze** | **Parquet** (snappy), particionado por data | primeiro ponto em que o dado ganha schema e tipos; tudo a jusante lê daqui |
| **Silver** | **Parquet** limpo/deduplicado, particionado | consultas exploratórias já pagam o custo reduzido |
| **Gold** | **Parquet** agregado (ou tabela do DW) | resultados prontos para BI |

Diretrizes práticas que este benchmark sustenta:

1. **Converta para Parquet no Bronze, não depois.** Cada consulta a jusante que ainda toque CSV paga de novo o custo de parse. A conversão é um custo único; a leitura acontece milhares de vezes.
2. **Particione por `sale_date`** (`ano=2024/mes=03/`). Partição é pushdown no nível de *diretório*: arquivos fora da janela nem chegam a ser abertos. É o degrau seguinte ao que medimos aqui, e é o que faltava para o pushdown render em tempo na seção 3.3.
3. **Cuide do tamanho do row group.** Usamos 100.000 linhas (~128 MB descomprimidos é o alvo clássico). Row groups grandes demais matam o pushdown — não há o que podar; pequenos demais inflam o footer e geram I/O fragmentado.
4. **Ordene (ou faça cluster) pelas colunas de filtro mais comuns** antes de gravar. Estatísticas min/max só podam se os dados estiverem correlacionados com a ordem física — foi o que tornou o pushdown por data eficaz aqui, e a falta disso é o que torna o filtro por `payment_method` inútil para poda.
5. **Evite arquivos pequenos.** Milhares de Parquets de poucos KB custam mais em abertura de footer do que economizam; consolide (*compaction*) para 128-512 MB.

## 5. Lendo com otimizações: equivalente SQL (DuckDB)

O DuckDB aplica projection e pushdown **automaticamente** a partir do SQL: lê o footer, poda row groups pelas estatísticas e materializa só as colunas do `SELECT`/`WHERE`. O mesmo que fizemos à mão com PyArrow, só que declarativo.

```sql
-- Q1: agregacao + filtro
--   projection: le so product_category e order_value
--   pushdown  : order_value >= 100 vira filtro de row group + scan
SELECT product_category,
       SUM(order_value) AS total_vendas,
       COUNT(*)         AS quantidade_pedidos
FROM   read_parquet('data/dados_vendas.parquet')
WHERE  order_value >= 100
GROUP  BY product_category
ORDER  BY total_vendas DESC;

-- Q3: filtro complexo (projection + pushdown combinados)
SELECT region,
       AVG(order_value) AS media_valor,
       MAX(order_value) AS max_valor,
       MIN(order_value) AS min_valor
FROM   read_parquet('data/dados_vendas.parquet')
WHERE  payment_method = 'Pix'
  AND  sale_date > DATE '2024-01-01'
GROUP  BY region;

-- Q4: top 10 clientes (projection pura: 2 de 10 colunas)
SELECT customer_id, SUM(order_value) AS total_gasto
FROM   read_parquet('data/dados_vendas.parquet')
GROUP  BY customer_id
ORDER  BY total_gasto DESC
LIMIT  10;

-- Conferindo que a poda aconteceu: o plano mostra 'Filters: ...'
-- no PARQUET_SCAN e a contagem de row groups efetivamente lidos.
EXPLAIN ANALYZE
SELECT region, AVG(order_value)
FROM   read_parquet('data/dados_vendas.parquet')
WHERE  payment_method = 'Pix' AND sale_date > DATE '2024-01-01'
GROUP  BY region;

-- Conversao CSV -> Parquet particionado (o passo Raw -> Bronze)
COPY (SELECT * FROM read_csv_auto('data/dados_vendas.csv'))
TO 'lake/bronze/vendas'
(FORMAT PARQUET, COMPRESSION SNAPPY,
 PARTITION_BY (year(sale_date), month(sale_date)),
 ROW_GROUP_SIZE 100000);
```

Equivalente em PyArrow — o que o `script_benchmark.py` executa de fato:

```python
import pyarrow.parquet as pq

tabela = pq.read_table(
    'data/dados_vendas.parquet',
    columns=['region', 'order_value'],                    # Column Projection
    filters=[('payment_method', '==', 'Pix'),             # Predicate Pushdown
             ('sale_date', '>', datetime(2024, 1, 1))],
)
resultado = tabela.group_by('region').aggregate([('order_value', 'mean')])
```

## Anexo: resultados das consultas

Os três caminhos são comparados célula a célula a cada execução (`validar()`); abaixo, o retorno de cada consulta (as 10 primeiras linhas, quando há mais que isso).

**Q1 — Total de vendas por categoria, apenas vendas >= R$ 100**  
Retorna `product_category, total_vendas, quantidade_pedidos` · 8 linhas · validação: OK

| product_category | total_vendas | quantidade_pedidos |
|---|---|---|
| Alimentos | 10,610,736.84 | 55,290 |
| Beleza | 3,410,364.10 | 17,701 |
| Brinquedos | 1,904,569.11 | 9,829 |
| Casa e Jardim | 5,809,566.00 | 30,102 |
| Eletronicos | 8,753,033.74 | 45,449 |
| Esportes | 4,370,500.59 | 22,742 |
| Livros | 3,817,774.75 | 19,858 |
| Roupas | 9,671,334.09 | 50,277 |

**Q2 — Vendas por região, método de pagamento e status**  
Retorna `region, payment_method, status, total, count` · 45 linhas · validação: OK

| region | payment_method | status | total | count |
|---|---|---|---|---|
| Centro-Oeste | Boleto | Cancelado | 59,198.74 | 714 |
| Centro-Oeste | Boleto | Concluido | 822,030.22 | 10,078 |
| Centro-Oeste | Boleto | Pendente | 74,051.99 | 908 |
| Centro-Oeste | Cartao | Cancelado | 221,548.92 | 2,755 |
| Centro-Oeste | Cartao | Concluido | 3,287,467.22 | 40,151 |
| Centro-Oeste | Cartao | Pendente | 304,759.49 | 3,759 |
| Centro-Oeste | Pix | Cancelado | 156,595.66 | 1,864 |
| Centro-Oeste | Pix | Concluido | 2,182,672.64 | 26,962 |
| Centro-Oeste | Pix | Pendente | 196,353.93 | 2,467 |
| Nordeste | Boleto | Cancelado | 140,564.35 | 1,713 |

**Q3 — Média/máx/mín por região, apenas Pix e sale_date > 2024-01-01**  
Retorna `region, media_valor, max_valor, min_valor` · 5 linhas · validação: OK

| region | media_valor | max_valor | min_valor |
|---|---|---|---|
| Centro-Oeste | 80.84 | 1,839.59 | 5.00 |
| Nordeste | 82.61 | 1,915.20 | 5.00 |
| Norte | 81.73 | 1,288.45 | 5.00 |
| Sudeste | 81.68 | 2,861.87 | 5.00 |
| Sul | 82.21 | 1,551.47 | 5.00 |

**Q4 — Top 10 clientes por gasto total**  
Retorna `customer_id, total_gasto` · 10 linhas · validação: OK

| customer_id | total_gasto |
|---|---|
| 27474 | 5,806.27 |
| 20843 | 5,447.83 |
| 37074 | 5,253.98 |
| 42470 | 5,250.09 |
| 44212 | 5,070.94 |
| 41349 | 5,067.28 |
| 44403 | 4,975.50 |
| 1466 | 4,897.88 |
| 13575 | 4,706.90 |
| 36446 | 4,657.25 |
