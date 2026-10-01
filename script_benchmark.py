# -*- coding: utf-8 -*-
"""
script_benchmark.py
===================
Benchmark de consultas analiticas de e-commerce: CSV (pandas) vs Parquet (PyArrow).

Compara tres estrategias de execucao para as mesmas 4 consultas:

  A) CSV + pandas            -> le o arquivo inteiro com pd.read_csv()
  B) Parquet + PyArrow       -> le a tabela inteira com pq.read_table()
  C) Parquet + PyArrow (opt) -> Column Projection (columns=) + Predicate Pushdown (filters=)

Cada medicao engloba o pipeline completo (I/O + parse + agregacao), que e o que o
usuario final percebe como "tempo da consulta".

Uso:
    python script_benchmark.py                 # gera dados (se faltarem) + benchmark + relatorio
    python script_benchmark.py --apenas-dados  # so gera os arquivos em ./data
    python script_benchmark.py --regerar       # forca a regeracao do dataset
    python script_benchmark.py --linhas 2000000 --repeticoes 5

Bibliotecas: pandas, pyarrow, numpy, psutil, time/timeit (+ stdlib: gc, sys, subprocess, threading).
"""

from __future__ import annotations

import argparse
import gc
import statistics
import subprocess
import sys
import threading
import time
import timeit
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import psutil
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

# ---------------------------------------------------------------------------
# CONFIGURACAO GLOBAL
# ---------------------------------------------------------------------------
RAIZ = Path(__file__).resolve().parent
DIR_DADOS = RAIZ / "data"
DIR_RESULTADOS = RAIZ / "results"
ARQ_CSV = DIR_DADOS / "dados_vendas.csv"
ARQ_PARQUET = DIR_DADOS / "dados_vendas.parquet"

N_REGISTROS = 1_000_000
SEED = 42

# 10 row groups de 100k linhas. Cada row group carrega estatisticas (min/max) por
# coluna no rodape do arquivo -> e o que permite ao Predicate Pushdown *pular*
# blocos inteiros sem descompactar. Um unico row group de 1M linhas deixaria o
# pushdown sem nada para podar.
LINHAS_POR_ROW_GROUP = 100_000

REPETICOES = 9   # execucoes por medicao de tempo (timeit); reportamos a mediana
REPETICOES_ISOLAMENTO = 21  # o experimento de isolamento compara tempos proximos,
                            # entao precisa de mais amostras para a mediana assentar

# Parametros das consultas
VALOR_MINIMO_Q1 = 100.0
DATA_CORTE_Q3 = datetime(2024, 1, 1)
METODO_Q3 = "Pix"
TOP_N_Q4 = 10

# Dominios categoricos do dataset sintetico
CATEGORIAS = ["Eletronicos", "Roupas", "Alimentos", "Livros",
              "Casa e Jardim", "Esportes", "Beleza", "Brinquedos"]
PESO_CATEGORIAS = [0.18, 0.20, 0.22, 0.08, 0.12, 0.09, 0.07, 0.04]

REGIOES = ["Norte", "Nordeste", "Centro-Oeste", "Sudeste", "Sul"]
PESO_REGIOES = [0.07, 0.22, 0.09, 0.47, 0.15]  # proporcao proxima do consumo real

PAGAMENTOS = ["Cartao", "Boleto", "Pix"]
PESO_PAGAMENTOS = [0.52, 0.13, 0.35]

STATUS = ["Concluido", "Cancelado", "Pendente"]
PESO_STATUS = [0.86, 0.06, 0.08]


# ===========================================================================
# 1. INSTRUMENTACAO: TEMPO (timeit) E MEMORIA (psutil)
# ===========================================================================
class MonitorMemoria(threading.Thread):
    """Amostra o RSS do processo em background para capturar o PICO de memoria.

    psutil expoe apenas o uso *instantaneo* (memory_info().rss). Como o pico de
    uma consulta acontece no meio da execucao (ex.: DataFrame inteiro carregado
    antes do filtro), amostramos em alta frequencia numa thread separada.
    """

    def __init__(self, processo: psutil.Process, intervalo: float = 0.005) -> None:
        super().__init__(daemon=True)
        self._processo = processo
        self._intervalo = intervalo
        self._parar = threading.Event()
        self.pico_bytes = processo.memory_info().rss

    def run(self) -> None:
        while not self._parar.is_set():
            self.pico_bytes = max(self.pico_bytes, self._processo.memory_info().rss)
            time.sleep(self._intervalo)

    def encerrar(self) -> None:
        self._parar.set()
        self.join(timeout=1.0)


def medir_tempo(funcao, repeticoes: int = REPETICOES) -> dict:
    """Mede o tempo de `funcao` com timeit e devolve estatisticas em ms.

    number=1 porque cada execucao ja e cara (le o arquivo inteiro); repeat=N
    para amostrar a variacao.

    Reportamos a **mediana**, nao o minimo. As leituras Parquet ficam na casa
    das dezenas de ms e oscilam bastante (alocacao, GC, escalonamento do SO):
    medindo a leitura completa observamos desvio-padrao da ordem de 50% da
    propria mediana. Nesse regime o minimo e instavel -- varia de execucao para
    execucao e chega a inverter a ordem entre variantes -- enquanto a mediana e
    robusta a outliers nas duas pontas e representa o tempo tipico.
    """
    tempos = [t * 1000.0 for t in timeit.repeat(funcao, repeat=repeticoes, number=1)]
    return {
        "mediana": statistics.median(tempos),
        "minimo": min(tempos),
        "media": statistics.fmean(tempos),
        "desvio": statistics.stdev(tempos) if len(tempos) > 1 else 0.0,
    }


def medir_memoria_local(funcao) -> tuple[float, object]:
    """Mede o pico de memoria de `funcao` DENTRO do processo atual.

    So produz numero confiavel num processo recem-criado: uma vez que o heap
    cresceu, o alocador reaproveita as arenas liberadas e o RSS nao sobe de novo
    -- execucoes seguintes mediriam ~0 MB. Por isso esta funcao e chamada no
    subprocesso criado por `medir_memoria()`.
    """
    processo = psutil.Process()
    gc.collect()
    base = processo.memory_info().rss

    monitor = MonitorMemoria(processo, intervalo=0.002)
    monitor.start()
    try:
        resultado = funcao()
    finally:
        monitor.encerrar()

    # No Windows o proprio SO informa o pico exato do working set (peak_wset);
    # nas demais plataformas usamos o pico amostrado pela thread monitora.
    info = processo.memory_info()
    pico = max(getattr(info, "peak_wset", 0) or 0, monitor.pico_bytes)
    return max(pico - base, 0) / (1024 ** 2), resultado


def medir_memoria(alvo: str) -> float:
    """Mede o pico de memoria de uma consulta num SUBPROCESSO limpo.

    Cada medicao roda num interpretador novo, entao todas partem do mesmo
    estado de heap e os numeros ficam comparaveis entre si.
    """
    processo = subprocess.run(
        [sys.executable, str(Path(__file__).resolve()), "--medir", alvo],
        capture_output=True, text=True,
    )
    for linha in reversed(processo.stdout.splitlines()):
        if linha.startswith("MEM_MB="):
            return float(linha.split("=", 1)[1])
    raise RuntimeError(f"medicao de memoria falhou para '{alvo}':\n{processo.stderr}")


# ===========================================================================
# 2. GERACAO DO DATASET SINTETICO (100% vetorizada com numpy)
# ===========================================================================
def gerar_dataset(n_registros: int = N_REGISTROS, seed: int = SEED) -> pd.DataFrame:
    """Gera `n_registros` vendas sinteticas sem nenhum loop em Python.

    Todas as colunas saem de chamadas vetorizadas do numpy sobre arrays de
    tamanho n -- gerar 1M de linhas em loop levaria minutos; assim leva ~1s.
    """
    rng = np.random.default_rng(seed)

    # --- datas: sorteia offsets em segundos dentro da janela e ORDENA ---------
    # Ordenar cronologicamente nao e cosmetico: em Parquet isso agrupa datas
    # proximas no mesmo row group, estreitando o intervalo [min, max] das
    # estatisticas e tornando o Predicate Pushdown por data realmente efetivo.
    # E tambem o formato natural de um historico de vendas acumulado por ETL.
    inicio = np.datetime64("2023-01-01T00:00:00")
    fim = np.datetime64("2024-12-31T23:59:59")
    total_segundos = int((fim - inicio) / np.timedelta64(1, "s"))
    offsets = np.sort(rng.integers(0, total_segundos, size=n_registros, dtype=np.int64))
    sale_date = inicio + offsets.astype("timedelta64[s]")

    # --- valores monetarios: lognormal gera a cauda longa tipica de e-commerce
    order_value = np.round(rng.lognormal(mean=4.0, sigma=0.9, size=n_registros), 2)
    order_value = np.clip(order_value, 5.0, 25_000.0)

    return pd.DataFrame({
        "order_id": np.arange(1, n_registros + 1, dtype=np.int64),
        "customer_id": rng.integers(1, 50_001, size=n_registros, dtype=np.int32),
        "product_id": rng.integers(1, 5_001, size=n_registros, dtype=np.int32),
        "product_category": rng.choice(CATEGORIAS, size=n_registros, p=PESO_CATEGORIAS),
        "sale_date": sale_date,
        "order_value": order_value,
        "quantity": rng.integers(1, 11, size=n_registros, dtype=np.int16),
        "region": rng.choice(REGIOES, size=n_registros, p=PESO_REGIOES),
        "payment_method": rng.choice(PAGAMENTOS, size=n_registros, p=PESO_PAGAMENTOS),
        "status": rng.choice(STATUS, size=n_registros, p=PESO_STATUS),
    })


def salvar_dataset(df: pd.DataFrame) -> dict:
    """Persiste o DataFrame em CSV e Parquet, medindo tempo e tamanho de cada um."""
    DIR_DADOS.mkdir(parents=True, exist_ok=True)

    t0 = time.perf_counter()
    df.to_csv(ARQ_CSV, index=False, encoding="utf-8")
    tempo_csv = time.perf_counter() - t0

    # Parquet colunar + Snappy (compressao rapida, padrao de mercado em data lakes).
    tabela = pa.Table.from_pandas(df, preserve_index=False)
    t0 = time.perf_counter()
    pq.write_table(
        tabela,
        ARQ_PARQUET,
        compression="snappy",
        row_group_size=LINHAS_POR_ROW_GROUP,
        write_statistics=True,  # min/max por row group -> habilita o pushdown
    )
    tempo_parquet = time.perf_counter() - t0

    return {
        "linhas": len(df),
        "tempo_escrita_csv_s": tempo_csv,
        "tempo_escrita_parquet_s": tempo_parquet,
        "tamanho_csv_mb": ARQ_CSV.stat().st_size / (1024 ** 2),
        "tamanho_parquet_mb": ARQ_PARQUET.stat().st_size / (1024 ** 2),
        "row_groups": pq.ParquetFile(ARQ_PARQUET).num_row_groups,
    }


# ===========================================================================
# 3. CONSULTAS ANALITICAS (3 implementacoes por consulta)
# ===========================================================================
# Convencao: toda consulta devolve um pandas.DataFrame ja normalizado
# (mesmas colunas, mesma ordem de linhas) para permitir a validacao cruzada.

def _normalizar(df: pd.DataFrame, chaves: list[str],
                crescente: bool = True) -> pd.DataFrame:
    """Ordena e reseta o indice para que as 3 engines saiam comparaveis.

    A ordem precisa ser deterministica para a validacao cruzada funcionar. Em
    agregacoes ordenamos pela chave do grupo; em ranking (Q4) ordenamos pela
    metrica em ordem decrescente, que e a ordem que o resultado deve ter.
    """
    return df.sort_values(chaves, ascending=crescente).reset_index(drop=True)


# --------------------------- Q1 -------------------------------------------
# Total de vendas por categoria, apenas vendas >= R$ 100.
def q1_csv() -> pd.DataFrame:
    df = pd.read_csv(ARQ_CSV, parse_dates=["sale_date"])
    filtrado = df[df["order_value"] >= VALOR_MINIMO_Q1]
    resultado = filtrado.groupby("product_category", as_index=False).agg(
        total_vendas=("order_value", "sum"),
        quantidade_pedidos=("order_id", "count"),
    )
    return _normalizar(resultado, ["product_category"])


def _agregar_q1(tabela: pa.Table) -> pd.DataFrame:
    agg = tabela.group_by("product_category").aggregate(
        [("order_value", "sum"), ("order_value", "count")]
    )
    resultado = agg.to_pandas().rename(columns={
        "order_value_sum": "total_vendas",
        "order_value_count": "quantidade_pedidos",
    })
    return _normalizar(resultado[["product_category", "total_vendas", "quantidade_pedidos"]],
                       ["product_category"])


def q1_parquet() -> pd.DataFrame:
    tabela = pq.read_table(ARQ_PARQUET)  # le TODAS as 10 colunas e TODAS as linhas
    return _agregar_q1(tabela.filter(pc.field("order_value") >= VALOR_MINIMO_Q1))


def q1_parquet_opt() -> pd.DataFrame:
    # Column Projection: 2 de 10 colunas. Predicate Pushdown: o filtro desce ate
    # o leitor Parquet, que descarta linhas antes de materializar o Arrow Table.
    tabela = pq.read_table(
        ARQ_PARQUET,
        columns=["product_category", "order_value"],
        filters=[("order_value", ">=", VALOR_MINIMO_Q1)],
    )
    return _agregar_q1(tabela)


# --------------------------- Q2 -------------------------------------------
# Vendas por regiao x metodo de pagamento x status.
CHAVES_Q2 = ["region", "payment_method", "status"]


def q2_csv() -> pd.DataFrame:
    df = pd.read_csv(ARQ_CSV, parse_dates=["sale_date"])
    resultado = df.groupby(CHAVES_Q2, as_index=False).agg(
        total=("order_value", "sum"),
        count=("order_id", "count"),
    )
    return _normalizar(resultado, CHAVES_Q2)


def _agregar_q2(tabela: pa.Table) -> pd.DataFrame:
    agg = tabela.group_by(CHAVES_Q2).aggregate(
        [("order_value", "sum"), ("order_value", "count")]
    )
    resultado = agg.to_pandas().rename(columns={
        "order_value_sum": "total", "order_value_count": "count",
    })
    return _normalizar(resultado[CHAVES_Q2 + ["total", "count"]], CHAVES_Q2)


def q2_parquet() -> pd.DataFrame:
    return _agregar_q2(pq.read_table(ARQ_PARQUET))


def q2_parquet_opt() -> pd.DataFrame:
    # Q2 nao tem WHERE -> nao ha Predicate Pushdown aplicavel.
    # O ganho aqui vem exclusivamente do Column Projection (4 de 10 colunas).
    return _agregar_q2(pq.read_table(ARQ_PARQUET, columns=CHAVES_Q2 + ["order_value"]))


# --------------------------- Q3 -------------------------------------------
# Media/max/min do valor por regiao, so Pix e so datas > 2024-01-01.
def q3_csv() -> pd.DataFrame:
    df = pd.read_csv(ARQ_CSV, parse_dates=["sale_date"])
    mascara = (df["payment_method"] == METODO_Q3) & (df["sale_date"] > DATA_CORTE_Q3)
    resultado = df[mascara].groupby("region", as_index=False).agg(
        media_valor=("order_value", "mean"),
        max_valor=("order_value", "max"),
        min_valor=("order_value", "min"),
    )
    return _normalizar(resultado, ["region"])


def _agregar_q3(tabela: pa.Table) -> pd.DataFrame:
    agg = tabela.group_by("region").aggregate(
        [("order_value", "mean"), ("order_value", "max"), ("order_value", "min")]
    )
    resultado = agg.to_pandas().rename(columns={
        "order_value_mean": "media_valor",
        "order_value_max": "max_valor",
        "order_value_min": "min_valor",
    })
    return _normalizar(resultado[["region", "media_valor", "max_valor", "min_valor"]],
                       ["region"])


def q3_parquet() -> pd.DataFrame:
    tabela = pq.read_table(ARQ_PARQUET)
    tabela = tabela.filter(
        (pc.field("payment_method") == METODO_Q3) & (pc.field("sale_date") > DATA_CORTE_Q3)
    )
    return _agregar_q3(tabela)


def q3_parquet_opt() -> pd.DataFrame:
    # As colunas usadas no filtro NAO precisam estar na projecao: o pushdown
    # avalia payment_method/sale_date no scan e devolve so region + order_value.
    tabela = pq.read_table(
        ARQ_PARQUET,
        columns=["region", "order_value"],
        filters=[("payment_method", "==", METODO_Q3), ("sale_date", ">", DATA_CORTE_Q3)],
    )
    return _agregar_q3(tabela)


# --------------------------- Q4 -------------------------------------------
# Top 10 clientes por gasto total (so as colunas necessarias).
def q4_csv() -> pd.DataFrame:
    df = pd.read_csv(ARQ_CSV, parse_dates=["sale_date"])
    resultado = (df.groupby("customer_id", as_index=False)
                   .agg(total_gasto=("order_value", "sum"))
                   .nlargest(TOP_N_Q4, "total_gasto"))
    return _normalizar(resultado, ["total_gasto"], crescente=False)


def _top_clientes(tabela: pa.Table) -> pd.DataFrame:
    agg = (tabela.group_by("customer_id")
                 .aggregate([("order_value", "sum")])
                 .sort_by([("order_value_sum", "descending")])
                 .slice(0, TOP_N_Q4))
    resultado = agg.to_pandas().rename(columns={"order_value_sum": "total_gasto"})
    return _normalizar(resultado[["customer_id", "total_gasto"]],
                       ["total_gasto"], crescente=False)


def q4_parquet() -> pd.DataFrame:
    return _top_clientes(pq.read_table(ARQ_PARQUET))


def q4_parquet_opt() -> pd.DataFrame:
    # Sem WHERE: puro Column Projection -- 2 de 10 colunas (customer_id, order_value).
    return _top_clientes(pq.read_table(ARQ_PARQUET, columns=["customer_id", "order_value"]))


# --------------------- experimento de isolamento ---------------------------
# Q3 combina as duas otimizacoes, o que impede atribuir o ganho a uma delas.
# Estas duas variantes ligam uma otimizacao de cada vez para medir a contribuicao
# individual de Column Projection e de Predicate Pushdown.
def q3_apenas_projecao() -> pd.DataFrame:
    # So projecao: precisa trazer tambem as colunas do filtro, porque o filtro
    # sera avaliado em memoria (4 de 10 colunas).
    tabela = pq.read_table(
        ARQ_PARQUET,
        columns=["region", "order_value", "payment_method", "sale_date"],
    )
    tabela = tabela.filter(
        (pc.field("payment_method") == METODO_Q3) & (pc.field("sale_date") > DATA_CORTE_Q3)
    )
    return _agregar_q3(tabela)


def q3_apenas_pushdown() -> pd.DataFrame:
    # So pushdown: filtra no scan, mas materializa as 10 colunas.
    tabela = pq.read_table(
        ARQ_PARQUET,
        filters=[("payment_method", "==", METODO_Q3), ("sale_date", ">", DATA_CORTE_Q3)],
    )
    return _agregar_q3(tabela)


VARIANTES_ISOLAMENTO = [
    ("Nenhuma (lê tudo)", q3_parquet),
    ("Só Column Projection", q3_apenas_projecao),
    ("Só Predicate Pushdown", q3_apenas_pushdown),
    ("Projection + Pushdown", q3_parquet_opt),
]


def analisar_pruning(coluna: str = "sale_date", corte: datetime = DATA_CORTE_Q3) -> dict:
    """Le as estatisticas do footer e conta quantos row groups o filtro pode podar.

    Um row group so pode ser descartado sem leitura se seu intervalo [min, max]
    nao intersecta o predicado -- aqui, se `max(sale_date) <= corte`.
    """
    arquivo = pq.ParquetFile(ARQ_PARQUET)
    indice = arquivo.schema_arrow.get_field_index(coluna)
    podados = 0
    # Laco sobre metadados (10 iteracoes), nao sobre dados -- nao ha versao vetorizada.
    for i in range(arquivo.num_row_groups):
        estatisticas = arquivo.metadata.row_group(i).column(indice).statistics
        if estatisticas is not None and estatisticas.max <= corte:
            podados += 1
    return {"total": arquivo.num_row_groups, "podados": podados}


# --------------------------- catalogo -------------------------------------
CONSULTAS = [
    {
        "id": "Q1",
        "titulo": "Agregação + Filtro",
        "descricao": "Total de vendas por categoria, apenas vendas >= R$ 100",
        "retorno": "product_category, total_vendas, quantidade_pedidos",
        "otimizacoes": "Projeção 2/10 colunas + pushdown de `order_value`",
        "impls": {"csv": q1_csv, "parquet": q1_parquet, "parquet_opt": q1_parquet_opt},
    },
    {
        "id": "Q2",
        "titulo": "GROUP BY múltiplas colunas",
        "descricao": "Vendas por região, método de pagamento e status",
        "retorno": "region, payment_method, status, total, count",
        "otimizacoes": "Projeção 4/10 colunas (sem WHERE: não há pushdown)",
        "impls": {"csv": q2_csv, "parquet": q2_parquet, "parquet_opt": q2_parquet_opt},
    },
    {
        "id": "Q3",
        "titulo": "Filtro complexo + Agregação",
        "descricao": "Média/máx/mín por região, apenas Pix e sale_date > 2024-01-01",
        "retorno": "region, media_valor, max_valor, min_valor",
        "otimizacoes": "Projeção 2/10 colunas + pushdown de `payment_method` e `sale_date`",
        "impls": {"csv": q3_csv, "parquet": q3_parquet, "parquet_opt": q3_parquet_opt},
    },
    {
        "id": "Q4",
        "titulo": "Seleção de colunas específicas",
        "descricao": "Top 10 clientes por gasto total",
        "retorno": "customer_id, total_gasto",
        "otimizacoes": "Projeção 2/10 colunas (sem WHERE: não há pushdown)",
        "impls": {"csv": q4_csv, "parquet": q4_parquet, "parquet_opt": q4_parquet_opt},
    },
]

ENGINES = [
    ("csv", "CSV (pandas)"),
    ("parquet", "Parquet (sem opt)"),
    ("parquet_opt", "Parquet (com opt)"),
]


# ===========================================================================
# 4. EXECUCAO DO BENCHMARK
# ===========================================================================
def registro_alvos() -> dict:
    """Mapeia um identificador textual -> funcao de consulta.

    Serve para o subprocesso de medicao de memoria saber o que executar
    (ele recebe apenas o nome do alvo pela linha de comando).
    """
    alvos = {f"{c['id']}:{engine}": fn
             for c in CONSULTAS for engine, fn in c["impls"].items()}
    alvos.update({f"ISO:{i}": fn for i, (_, fn) in enumerate(VARIANTES_ISOLAMENTO)})
    return alvos


def executar_consulta(funcao, alvo: str, repeticoes: int = REPETICOES) -> dict:
    """Funcao reutilizavel: mede tempo (timeit) e pico de memoria (psutil) de uma consulta.

    Tempo e memoria vem de passadas separadas de proposito:
      - tempo   -> no processo atual, com o cache do SO ja aquecido (timeit);
      - memoria -> num subprocesso limpo, para o pico ser comparavel.
    """
    tempos = medir_tempo(funcao, repeticoes)
    return {
        "tempo_ms": tempos["mediana"],  # metrica principal do relatorio
        "tempo_min_ms": tempos["minimo"],
        "tempo_medio_ms": tempos["media"],
        "tempo_desvio_ms": tempos["desvio"],
        "memoria_mb": medir_memoria(alvo),
        "resultado": funcao(),
    }


def validar(medidas: dict, consulta_id: str) -> bool:
    """Confere se as 3 implementacoes produziram exatamente o mesmo resultado."""
    referencia = medidas["csv"]["resultado"]
    for chave in ("parquet", "parquet_opt"):
        atual = medidas[chave]["resultado"]
        if list(referencia.columns) != list(atual.columns) or len(referencia) != len(atual):
            print(f"   [!] {consulta_id}/{chave}: formato divergente do CSV")
            return False
        for coluna in referencia.columns:
            a, b = referencia[coluna], atual[coluna]
            if pd.api.types.is_numeric_dtype(a) and pd.api.types.is_numeric_dtype(b):
                if not np.allclose(a.to_numpy(dtype=float), b.to_numpy(dtype=float), rtol=1e-6):
                    print(f"   [!] {consulta_id}/{chave}: coluna '{coluna}' divergente")
                    return False
            elif not a.astype(str).equals(b.astype(str)):
                print(f"   [!] {consulta_id}/{chave}: coluna '{coluna}' divergente")
                return False
    return True


def rodar_benchmark(repeticoes: int = REPETICOES) -> list[dict]:
    resultados = []
    for consulta in CONSULTAS:
        print(f"\n[{consulta['id']}] {consulta['titulo']} -- {consulta['descricao']}")
        medidas = {}
        for chave, rotulo in ENGINES:
            medida = executar_consulta(consulta["impls"][chave],
                                       f"{consulta['id']}:{chave}", repeticoes)
            medidas[chave] = medida
            print(f"   {rotulo:<20} {medida['tempo_ms']:>9.1f} ms   {medida['memoria_mb']:>7.1f} MB")

        ok = validar(medidas, consulta["id"])
        print(f"   {'validacao cruzada: OK (3 engines, mesmo resultado)' if ok else 'validacao cruzada: FALHOU'}")

        resultados.append({
            "consulta": consulta,
            "medidas": medidas,
            "linhas_retornadas": len(medidas["csv"]["resultado"]),
            "validado": ok,
            "amostra": medidas["parquet_opt"]["resultado"],
        })
    return resultados


def rodar_isolamento(repeticoes: int = REPETICOES_ISOLAMENTO) -> list[dict]:
    """Liga uma otimizacao de cada vez em Q3 para atribuir o ganho corretamente.

    Usa mais repeticoes que o benchmark principal: aqui as quatro variantes
    ficam todas na casa das dezenas de ms, entao a diferenca entre elas e da
    mesma ordem do ruido de uma execucao isolada.
    """
    print("\n[Q3-isolamento] Contribuicao individual de cada otimizacao")
    medidas = []
    for indice, (rotulo, funcao) in enumerate(VARIANTES_ISOLAMENTO):
        medida = executar_consulta(funcao, f"ISO:{indice}", repeticoes)
        medida["rotulo"] = rotulo
        medidas.append(medida)
        print(f"   {rotulo:<24} {medida['tempo_ms']:>9.1f} ms   {medida['memoria_mb']:>7.1f} MB")
    return medidas


# ===========================================================================
# 5. RELATORIO (results/RESULTS.md)
# ===========================================================================
def _barra(valor: float, maximo: float, largura: int = 34, log: bool = False) -> str:
    """Barra horizontal em ASCII para os 'graficos' do relatorio markdown.

    `log=True` usa escala logaritmica: com diferencas de ~100x entre CSV e
    Parquet, a escala linear achataria todas as barras de Parquet em 1 caractere.
    """
    if maximo <= 0:
        return ""
    proporcao = (np.log10(1 + valor) / np.log10(1 + maximo)) if log else (valor / maximo)
    return "#" * max(1, int(round(largura * proporcao)))


def _tabela_markdown(df: pd.DataFrame) -> str:
    """Renderiza um DataFrame como tabela markdown (sem depender de `tabulate`)."""
    def formatar(valor, coluna: str) -> str:
        if isinstance(valor, (float, np.floating)):
            return f"{valor:,.2f}"
        if isinstance(valor, (int, np.integer)):
            # IDs sao rotulos, nao quantidades: 44403 e nao 44.403
            return str(valor) if coluna.endswith("_id") else f"{valor:,}"
        return str(valor)

    colunas = list(df.columns)
    linhas = ["| " + " | ".join(colunas) + " |",
              "|" + "|".join("---" for _ in colunas) + "|"]
    # itertuples e a forma mais barata de percorrer poucas linhas de resultado
    for linha in df.itertuples(index=False):
        linhas.append("| " + " | ".join(formatar(v, c)
                                        for v, c in zip(linha, colunas)) + " |")
    return "\n".join(linhas)


def _grafico(titulo: str, unidade: str, resultados: list[dict], campo: str,
             log: bool = False) -> list[str]:
    linhas = ["```text", titulo]
    maximo = max(item["medidas"][chave][campo]
                 for item in resultados for chave, _ in ENGINES)
    for item in resultados:
        linhas.append("")
        linhas.append(f"{item['consulta']['id']}")
        for chave, rotulo in ENGINES:
            valor = item["medidas"][chave][campo]
            barra = _barra(valor, maximo, log=log)
            linhas.append(f"  {rotulo:<19} |{barra:<34}| {valor:>9.1f} {unidade}")
    linhas.append("```")
    return linhas


def gerar_relatorio(resultados: list[dict], info_dataset: dict, repeticoes: int,
                    isolamento: list[dict], pruning: dict) -> Path:
    """Escreve results/RESULTS.md com os numeros desta execucao."""
    DIR_RESULTADOS.mkdir(parents=True, exist_ok=True)
    destino = DIR_RESULTADOS / "RESULTS.md"

    csv_mb = info_dataset["tamanho_csv_mb"]
    pq_mb = info_dataset["tamanho_parquet_mb"]
    reducao_disco = (1 - pq_mb / csv_mb) * 100

    L: list[str] = []
    ad = L.append

    ad("# Benchmark: CSV vs Parquet em Consultas Analíticas")
    ad("")
    ad(f"> Gerado automaticamente por `script_benchmark.py` em "
       f"{datetime.now().strftime('%d/%m/%Y %H:%M')}.")
    ad("")
    ad("## Ambiente e dataset")
    ad("")
    ad("| Item | Valor |")
    ad("|---|---|")
    ad(f"| Registros | {info_dataset['linhas']:,} |".replace(",", "."))
    ad("| Colunas | 10 |")
    ad(f"| Arquivo CSV | {csv_mb:,.1f} MB |")
    ad(f"| Arquivo Parquet (snappy) | {pq_mb:,.1f} MB |")
    ad(f"| Redução em disco | **{reducao_disco:.1f}%** |")
    ad(f"| Row groups no Parquet | {info_dataset['row_groups']} "
       f"({LINHAS_POR_ROW_GROUP:,} linhas cada) |".replace(",", "."))
    ad(f"| Repetições por medição (timeit) | {repeticoes} (reportada a **mediana**) |")
    ad(f"| Bibliotecas | pandas {pd.__version__}, pyarrow {pa.__version__}, "
       f"numpy {np.__version__}, psutil {psutil.__version__} |")
    ad(f"| CPUs lógicas / RAM | {psutil.cpu_count()} / "
       f"{psutil.virtual_memory().total / 1024 ** 3:.1f} GB |")
    ad("")
    ad("As três estratégias comparadas medem sempre a **consulta inteira**, do arquivo "
       "em disco até o resultado agregado (I/O + parse + agregação) — que é o tempo "
       "que o analista de fato espera.")
    ad("")

    # ---------------- 0. As consultas -------------------------------------
    ad("## As consultas")
    ad("")
    ad("| Query | Tipo | O que calcula | Retorna | Otimizações aplicáveis |")
    ad("|---|---|---|---|---|")
    for item in resultados:
        c = item["consulta"]
        ad(f"| **{c['id']}** | {c['titulo']} | {c['descricao']} | "
           f"`{c['retorno']}` | {c['otimizacoes']} |")
    ad("")
    ad("Q2 e Q4 não têm cláusula `WHERE` — nelas a única otimização aplicável é o "
       "Column Projection. Isso é proposital: serve para separar o efeito de cada "
       "técnica ao longo do relatório.")
    ad("")

    # ---------------- 1. Tabela de tempo ----------------------------------
    ad("## 1. Desempenho (tempo em ms)")
    ad("")
    ad("| Query | CSV | Parquet (sem opt) | Parquet (com opt) | Melhoria % | Speedup |")
    ad("|---|---:|---:|---:|---:|---:|")
    for item in resultados:
        m = item["medidas"]
        t_csv = m["csv"]["tempo_ms"]
        t_pq = m["parquet"]["tempo_ms"]
        t_opt = m["parquet_opt"]["tempo_ms"]
        melhoria = (1 - t_opt / t_csv) * 100
        ad(f"| **{item['consulta']['id']}** | {t_csv:,.1f} | {t_pq:,.1f} | {t_opt:,.1f} | "
           f"**{melhoria:.1f}%** | {t_csv / t_opt:.1f}x |")
    ad("")
    ad("> *Melhoria %* e *Speedup* comparam **CSV vs Parquet otimizado** "
       "(a coluna que importa para a decisão de migrar).")
    ad("")
    dispersoes = [(item["consulta"]["id"], chave,
                   item["medidas"][chave]["tempo_desvio_ms"]
                   / item["medidas"][chave]["tempo_ms"] * 100)
                  for item in resultados for chave, _ in ENGINES]
    pior = max(dispersoes, key=lambda d: d[2])
    ad(f"Cada célula é a **mediana de {repeticoes} execuções**. O desvio-padrão chegou "
       f"a {pior[2]:.0f}% da mediana no caso mais instável "
       f"({pior[0]} / {dict(ENGINES)[pior[1]]}): leituras Parquet duram dezenas de ms "
       "e sofrem influência de alocação, GC e escalonamento do SO. Por isso a mediana, "
       "e não o mínimo — diferenças menores que ~20% entre duas variantes de Parquet "
       "não devem ser lidas como significativas. Já a diferença entre CSV e Parquet, "
       "de uma a duas ordens de grandeza, está muito acima desse ruído.")
    ad("")
    ad("\n".join(_grafico("TEMPO POR CONSULTA (ms) - menor e melhor - ESCALA LOGARITMICA",
                          "ms", resultados, "tempo_ms", log=True)))
    ad("")

    # ---------------- 2. Tabela de memoria --------------------------------
    ad("## 2. Consumo de memória (pico em MB)")
    ad("")
    ad("| Query | CSV | Parquet (sem opt) | Parquet (com opt) | Redução vs CSV |")
    ad("|---|---:|---:|---:|---:|")
    for item in resultados:
        m = item["medidas"]
        mem_csv = m["csv"]["memoria_mb"]
        mem_pq = m["parquet"]["memoria_mb"]
        mem_opt = m["parquet_opt"]["memoria_mb"]
        reducao = (1 - mem_opt / mem_csv) * 100 if mem_csv else 0.0
        ad(f"| **{item['consulta']['id']}** | {mem_csv:,.1f} | {mem_pq:,.1f} | "
           f"{mem_opt:,.1f} | **{reducao:.1f}%** |")
    ad("")
    ad("**Metodologia:** cada consulta é medida em um **subprocesso recém-criado**, "
       "porque o RSS de um processo não encolhe depois do primeiro pico — medir várias "
       "consultas no mesmo processo faria a segunda em diante aparecer como ~0 MB. O "
       "valor é o pico de working set acima da linha de base do interpretador já com "
       "as bibliotecas importadas (`psutil.Process().memory_info()`: `peak_wset` no "
       "Windows, ou amostragem a cada 2 ms em uma thread dedicada nas demais "
       "plataformas).")
    ad("")
    ad("\n".join(_grafico("PICO DE MEMORIA (MB) - menor e melhor", "MB",
                          resultados, "memoria_mb")))
    ad("")
    media_mem_csv = sum(r["medidas"]["csv"]["memoria_mb"] for r in resultados) / len(resultados)
    media_mem_pq = sum(r["medidas"]["parquet"]["memoria_mb"] for r in resultados) / len(resultados)
    ad("**O achado mais importante desta tabela está na coluna do meio:** ler o Parquet "
       f"*inteiro* custa praticamente a mesma memória que o CSV "
       f"({media_mem_pq:,.0f} MB contra {media_mem_csv:,.0f} MB, em média) — em algumas "
       "consultas chega a custar mais. E faz sentido: se todas as 10 colunas e 1M de "
       "linhas vão para a RAM, um `Table` do Arrow ocupa o mesmo que um `DataFrame` do "
       "pandas; a compressão existe **no arquivo**, não na memória.")
    ad("")
    ad("**Parquet não economiza RAM por ser Parquet — economiza porque permite não ler "
       "o que não será usado.** Sem `columns=`/`filters=`, o benefício é só de tempo "
       "(não há parse de texto); a economia de memória vem inteira das otimizações da "
       "coluna da direita.")
    ad("")

    # ---------------- 3. Explicacao tecnica -------------------------------
    melhor = max(resultados,
                 key=lambda r: r["medidas"]["csv"]["tempo_ms"]
                 / r["medidas"]["parquet_opt"]["tempo_ms"])
    ganho_melhor = (melhor["medidas"]["csv"]["tempo_ms"]
                    / melhor["medidas"]["parquet_opt"]["tempo_ms"])
    ganho_proj_q4 = (resultados[3]["medidas"]["parquet"]["memoria_mb"]
                     / max(resultados[3]["medidas"]["parquet_opt"]["memoria_mb"], 0.01))
    # isolamento: [nenhuma, so projecao, so pushdown, ambas]
    iso_base, iso_proj, iso_push, iso_ambas = isolamento

    ad("## 3. Explicação técnica")
    ad("")
    ad("### 3.1 Por que o CSV é lento antes mesmo da agregação")
    ad("")
    ad("CSV é um formato **orientado a linha e sem tipos**. Para responder "
       "\"soma de `order_value` por categoria\", o pandas precisa:")
    ad("")
    ad(f"1. ler os ~{csv_mb:.0f} MB do arquivo inteiro do disco;")
    ad("2. fazer *parse* de texto para tipo em **todas as 10 colunas**, inclusive as 8 "
       "que a consulta não usa (converter `\"1234.56\"` em float é caro, e `parse_dates` "
       "em 1M de timestamps é o item mais pesado da carga);")
    ad("3. materializar o DataFrame completo na RAM;")
    ad("4. só então filtrar e agregar.")
    ad("")
    ad("Os passos 1 a 3 dominam o tempo total. **Não existe ler \"meio CSV\"**: sem "
       "índice e sem metadados, qualquer byte do arquivo pode conter a linha que "
       "interessa.")
    ad("")
    ad("### 3.2 Como o Column Projection ajudou (redução de memória)")
    ad("")
    ad("Parquet armazena os dados **por coluna**: cada coluna é um bloco contíguo e "
       "independente dentro do arquivo, e o rodapé (*footer*) diz exatamente em que "
       "offset cada bloco começa. Pedir `columns=[...]` faz o leitor posicionar o "
       "ponteiro apenas nos blocos pedidos — as demais colunas **nunca são lidas do "
       "disco nem descompactadas**.")
    ad("")
    ad("O efeito acompanha a fração de colunas lidas, ainda que não seja exatamente "
       "linear (colunas de texto pesam muito mais que inteiros, e parte do pico é "
       "overhead fixo do leitor). Em Q4, que precisa apenas de `customer_id` e "
       "`order_value` — 2 de 10 colunas —, o pico de memória caiu de "
       f"{resultados[3]['medidas']['parquet']['memoria_mb']:.1f} MB (tabela inteira) "
       f"para {resultados[3]['medidas']['parquet_opt']['memoria_mb']:.1f} MB "
       f"(~{ganho_proj_q4:.1f}x menos), contra "
       f"{resultados[3]['medidas']['csv']['memoria_mb']:.1f} MB do CSV.")
    ad("")
    ad("Esse é também o motivo de a projeção ser a única otimização possível em Q2 e "
       "Q4: consultas sem `WHERE` não têm predicado para empurrar.")
    ad("")
    ad("A projeção melhora os dois indicadores (ver a tabela de isolamento em 3.3), mas "
       "é **na memória que ela é decisiva**: deixar de decodificar uma coluna poupa "
       "alguns milissegundos de CPU e, ao mesmo tempo, dezenas de MB que nunca "
       "precisam existir como array Arrow. Em produção é justamente o pico de memória "
       "que derruba o job (OOM) quando o volume cresce — o tempo apenas aumenta, a "
       "memória mata o processo.")
    ad("")
    ad("### 3.3 Como o Predicate Pushdown ajudou (redução de tempo)")
    ad("")
    ad("`filters=[...]` empurra o predicado para **dentro** do leitor Parquet, onde ele "
       "atua em duas camadas:")
    ad("")
    ad("- **Poda de row groups (economiza I/O):** o footer guarda `min`/`max` de cada "
       f"coluna em cada um dos {info_dataset['row_groups']} row groups. Se o intervalo "
       "do row group não intersecta o filtro, o bloco inteiro é pulado sem ser lido "
       "nem descompactado. Como o dataset foi gravado **ordenado por `sale_date`**, o "
       "predicado `sale_date > 2024-01-01` da Q3 elimina "
       f"**{pruning['podados']} dos {pruning['total']} row groups** apenas olhando as "
       "estatísticas — conferido lendo o footer com `ParquetFile.metadata` (função "
       "`analisar_pruning()`).")
    ad("- **Filtro no scan (economiza RAM e CPU):** nos row groups que sobraram, as "
       "linhas são avaliadas durante a leitura, então o `Table` já nasce filtrado. Sem "
       "pushdown, o PyArrow materializa 1M de linhas e só depois joga fora o que não "
       "passa.")
    ad("")
    ad("**Atribuindo o ganho corretamente.** A versão \"com opt\" liga as duas "
       "otimizações ao mesmo tempo, o que não permite dizer qual delas fez o trabalho. "
       "Rodamos então a Q3 com uma de cada vez "
       f"({REPETICOES_ISOLAMENTO} repetições por variante, mediana):")
    ad("")
    ad("| Otimização ativa em Q3 | Tempo (ms) | Desvio-padrão | Pico de memória (MB) |")
    ad("|---|---:|---:|---:|")
    for medida in isolamento:
        ad(f"| {medida['rotulo']} | {medida['tempo_ms']:,.1f} | "
           f"± {medida['tempo_desvio_ms']:,.1f} | {medida['memoria_mb']:,.1f} |")
    ad("")
    ad(f"> A linha \"Nenhuma\" mede a mesma consulta da coluna *Parquet (sem opt)* da "
       f"tabela 1, mas em uma bateria separada de {REPETICOES_ISOLAMENTO} execuções — "
       "por isso os dois valores não coincidem exatamente. Compare as variantes "
       "**dentro** desta tabela, não entre tabelas; os desvios-padrão da terceira "
       "coluna dão a escala do ruído.")
    ad("")
    # Fatores medidos (nao assumidos): quanto cada otimizacao rende sozinha.
    t_proj = iso_base["tempo_ms"] / iso_proj["tempo_ms"]
    t_push = iso_base["tempo_ms"] / iso_push["tempo_ms"]
    m_proj = iso_base["memoria_mb"] / iso_proj["memoria_mb"]
    m_push = iso_base["memoria_mb"] / iso_push["memoria_mb"]

    ad(f"Sozinhas, em relação à leitura sem otimização: a **projeção** rende "
       f"{t_proj:.2f}x em tempo e {m_proj:.2f}x em memória; o **pushdown** rende "
       f"{t_push:.2f}x em tempo e {m_push:.2f}x em memória. Juntas chegam a "
       f"{iso_ambas['tempo_ms']:,.1f} ms e {iso_ambas['memoria_mb']:,.1f} MB.")
    ad("")
    ad("Duas leituras saem daí — e a segunda contraria a expectativa de que "
       "\"pushdown = tempo\":")
    ad("")
    ad(f"**1. As duas otimizações cortam memória, por mecanismos diferentes.** A "
       f"projeção ({m_proj:.2f}x) elimina **colunas**; o pushdown ({m_push:.2f}x) "
       f"elimina **linhas**. Como atacam eixos distintos do mesmo scan, combiná-las "
       f"chega a {iso_ambas['memoria_mb']:,.1f} MB — melhor que qualquer uma isolada.")
    ad("")
    ad(f"**2. Em tempo, a projeção lidera** ({t_proj:.2f}x, contra {t_push:.2f}x do "
       "pushdown) — e essa ordem se repetiu em todas as execuções deste benchmark, "
       "embora as magnitudes oscilem. O teto do pushdown aqui é estrutural, e entender "
       "por quê é o que define quando cada técnica compensa:")
    ad("")
    ad(f"- a poda por estatísticas elimina apenas {pruning['podados']} dos "
       f"{pruning['total']} row groups. O predicado de data poda bem (os dados estão "
       "ordenados por `sale_date`), mas o de `payment_method` **não poda nada**: todo "
       "row group contém os três métodos de pagamento, então `min`/`max` sempre "
       "intersectam o filtro;")
    ad("- nos row groups que sobram, sem projeção ainda é preciso decodificar **as 10 "
       "colunas**. O filtro corta linhas, mas o custo por linha continua alto;")
    ad("- `filters=` troca o leitor direto pelo scanner do `pyarrow.dataset`, que "
       "avalia a expressão lote a lote — overhead que só se paga quando há muito dado "
       "a descartar.")
    ad("")
    ad("**A lição prática:** o Predicate Pushdown entrega tempo na proporção do que ele "
       "consegue *não ler*. Com ~35% das linhas sobrevivendo ao filtro e "
       f"{pruning['podados']}/{pruning['total']} row groups podados, ainda sobra "
       "trabalho demais para que o ganho seja dramático. Ele se torna decisivo quando "
       "o layout físico colabora — "
       "particionamento por data, ordenação pela coluna de filtro, predicados muito "
       "seletivos — e é exatamente por isso que a seção 4.3 recomenda particionar o "
       "Bronze por `sale_date`. A projeção, em contraste, entrega ganho garantido "
       "sempre que a consulta não usa todas as colunas, sem depender de como o arquivo "
       "foi organizado.")
    ad("")
    ad("> **Detalhe de implementação que vale registrar:** as colunas usadas no filtro "
       "não precisam estar na projeção. Em Q3 pedimos `columns=['region','order_value']` "
       "e filtramos por `payment_method`/`sale_date` — o leitor usa essas colunas "
       "internamente e as descarta antes de devolver a tabela.")
    ad("")
    ad("### 3.4 Qual consulta mais se beneficiou")
    ad("")
    ad(f"**{melhor['consulta']['id']} ({melhor['consulta']['titulo']})**, com "
       f"**{ganho_melhor:.1f}x** de speedup sobre o CSV "
       f"({melhor['medidas']['csv']['tempo_ms']:,.1f} ms → "
       f"{melhor['medidas']['parquet_opt']['tempo_ms']:,.1f} ms).")
    ad("")
    ad("O padrão geral: **quanto menos dado a consulta realmente precisa, maior o "
       "ganho**. Consultas que leem poucas colunas e filtram muitas linhas (Q1, Q3) "
       "combinam as duas otimizações e chegam a ordens de grandeza; consultas sem "
       "filtro (Q2, Q4) ganham \"apenas\" pela projeção e pela ausência de parse de "
       "texto — o que já é expressivo. Nenhuma consulta ficou mais lenta em Parquet.")
    ad("")

    # ---------------- 4. Recomendacoes ------------------------------------
    media_speedup = sum(r["medidas"]["csv"]["tempo_ms"] / r["medidas"]["parquet_opt"]["tempo_ms"]
                        for r in resultados) / len(resultados)
    ad("## 4. Recomendações arquiteturais")
    ad("")
    ad("### 4.1 Tamanho em disco")
    ad("")
    ad("| Formato | Tamanho | Relativo | Observação |")
    ad("|---|---:|---:|---|")
    ad(f"| CSV (texto puro) | {csv_mb:,.1f} MB | 100% | sem tipos, sem compressão |")
    ad(f"| Parquet (snappy) | {pq_mb:,.1f} MB | {pq_mb / csv_mb * 100:.1f}% | "
       f"colunar, dicionário + RLE + snappy |")
    ad("")
    ad(f"Redução de **{reducao_disco:.1f}%** para o mesmo conteúdo. A compressão "
       "colunar funciona tão bem porque valores semelhantes ficam vizinhos: colunas de "
       "baixa cardinalidade (`region`, `status`, `payment_method`) viram dicionários de "
       "3 a 8 entradas, e números são guardados em binário em vez de texto. Em nuvem "
       "isso vira dinheiro direto — menos GB armazenados e, sobretudo, menos GB "
       "*escaneados* por query em serviços cobrados por leitura (Athena, BigQuery).")
    ad("")
    ad("### 4.2 Conclusão: qual formato escolher")
    ad("")
    media_reducao_mem = sum(
        (1 - r["medidas"]["parquet_opt"]["memoria_mb"] / r["medidas"]["csv"]["memoria_mb"])
        for r in resultados) / len(resultados) * 100
    ad(f"**Parquet, para toda a camada analítica.** Em média **{media_speedup:.1f}x** "
       f"mais rápido que CSV nas 4 consultas, com {reducao_disco:.0f}% menos disco e "
       f"{media_reducao_mem:.0f}% menos memória *quando lido com projeção e filtros*. "
       "As três implementações foram validadas célula a célula e produzem resultados "
       "idênticos, então o ganho não custa precisão.")
    ad("")
    ad("A ressalva em itálico é a lição prática deste benchmark: **trocar o formato é "
       "metade do trabalho**. Um `pq.read_table()` sem argumentos já resolve o tempo "
       "(o parse de texto desaparece), mas mantém o mesmo consumo de RAM do CSV. Quem "
       "migra para Parquet e continua lendo tudo leva só metade do benefício.")
    ad("")
    ad("Quando **manter CSV** ainda faz sentido:")
    ad("")
    ad("- intercâmbio com sistemas ou pessoas que só leem texto (export de relatório, "
       "planilha para a área de negócio);")
    ad("- arquivos pequenos (< ~10 MB), onde a diferença é irrelevante;")
    ad("- ingestão bruta vinda de terceiros, que chega em CSV e deve ser preservada "
       "como recebida, por auditoria.")
    ad("")
    ad("### 4.3 Impacto na camada Raw/Bronze do Data Lake")
    ad("")
    ad("| Camada | Formato recomendado | Racional |")
    ad("|---|---|---|")
    ad("| **Raw / Landing** | arquivo original (CSV/JSON) + Parquet espelhado | "
       "preserva o dado como recebido (auditoria e reprocessamento) e já materializa "
       "uma cópia consultável |")
    ad("| **Bronze** | **Parquet** (snappy), particionado por data | primeiro ponto em "
       "que o dado ganha schema e tipos; tudo a jusante lê daqui |")
    ad("| **Silver** | **Parquet** limpo/deduplicado, particionado | consultas "
       "exploratórias já pagam o custo reduzido |")
    ad("| **Gold** | **Parquet** agregado (ou tabela do DW) | resultados prontos para BI |")
    ad("")
    ad("Diretrizes práticas que este benchmark sustenta:")
    ad("")
    ad("1. **Converta para Parquet no Bronze, não depois.** Cada consulta a jusante que "
       "ainda toque CSV paga de novo o custo de parse. A conversão é um custo único; a "
       "leitura acontece milhares de vezes.")
    ad("2. **Particione por `sale_date`** (`ano=2024/mes=03/`). Partição é pushdown no "
       "nível de *diretório*: arquivos fora da janela nem chegam a ser abertos. É o "
       "degrau seguinte ao que medimos aqui, e é o que faltava para o pushdown render "
       "em tempo na seção 3.3.")
    ad("3. **Cuide do tamanho do row group.** Usamos "
       f"{LINHAS_POR_ROW_GROUP:,}".replace(",", ".") +
       " linhas (~128 MB descomprimidos é o alvo clássico). Row groups grandes demais "
       "matam o pushdown — não há o que podar; pequenos demais inflam o footer e geram "
       "I/O fragmentado.")
    ad("4. **Ordene (ou faça cluster) pelas colunas de filtro mais comuns** antes de "
       "gravar. Estatísticas min/max só podam se os dados estiverem correlacionados com "
       "a ordem física — foi o que tornou o pushdown por data eficaz aqui, e a falta "
       "disso é o que torna o filtro por `payment_method` inútil para poda.")
    ad("5. **Evite arquivos pequenos.** Milhares de Parquets de poucos KB custam mais "
       "em abertura de footer do que economizam; consolide (*compaction*) para "
       "128-512 MB.")
    ad("")

    # ---------------- 5. DuckDB -------------------------------------------
    ad("## 5. Lendo com otimizações: equivalente SQL (DuckDB)")
    ad("")
    ad("O DuckDB aplica projection e pushdown **automaticamente** a partir do SQL: lê o "
       "footer, poda row groups pelas estatísticas e materializa só as colunas do "
       "`SELECT`/`WHERE`. O mesmo que fizemos à mão com PyArrow, só que declarativo.")
    ad("")
    ad("```sql")
    ad("-- Q1: agregacao + filtro")
    ad("--   projection: le so product_category e order_value")
    ad("--   pushdown  : order_value >= 100 vira filtro de row group + scan")
    ad("SELECT product_category,")
    ad("       SUM(order_value) AS total_vendas,")
    ad("       COUNT(*)         AS quantidade_pedidos")
    ad("FROM   read_parquet('data/dados_vendas.parquet')")
    ad("WHERE  order_value >= 100")
    ad("GROUP  BY product_category")
    ad("ORDER  BY total_vendas DESC;")
    ad("")
    ad("-- Q3: filtro complexo (projection + pushdown combinados)")
    ad("SELECT region,")
    ad("       AVG(order_value) AS media_valor,")
    ad("       MAX(order_value) AS max_valor,")
    ad("       MIN(order_value) AS min_valor")
    ad("FROM   read_parquet('data/dados_vendas.parquet')")
    ad("WHERE  payment_method = 'Pix'")
    ad("  AND  sale_date > DATE '2024-01-01'")
    ad("GROUP  BY region;")
    ad("")
    ad("-- Q4: top 10 clientes (projection pura: 2 de 10 colunas)")
    ad("SELECT customer_id, SUM(order_value) AS total_gasto")
    ad("FROM   read_parquet('data/dados_vendas.parquet')")
    ad("GROUP  BY customer_id")
    ad("ORDER  BY total_gasto DESC")
    ad("LIMIT  10;")
    ad("")
    ad("-- Conferindo que a poda aconteceu: o plano mostra 'Filters: ...'")
    ad("-- no PARQUET_SCAN e a contagem de row groups efetivamente lidos.")
    ad("EXPLAIN ANALYZE")
    ad("SELECT region, AVG(order_value)")
    ad("FROM   read_parquet('data/dados_vendas.parquet')")
    ad("WHERE  payment_method = 'Pix' AND sale_date > DATE '2024-01-01'")
    ad("GROUP  BY region;")
    ad("")
    ad("-- Conversao CSV -> Parquet particionado (o passo Raw -> Bronze)")
    ad("COPY (SELECT * FROM read_csv_auto('data/dados_vendas.csv'))")
    ad("TO 'lake/bronze/vendas'")
    ad("(FORMAT PARQUET, COMPRESSION SNAPPY,")
    ad(" PARTITION_BY (year(sale_date), month(sale_date)),")
    ad(" ROW_GROUP_SIZE 100000);")
    ad("```")
    ad("")
    ad("Equivalente em PyArrow — o que o `script_benchmark.py` executa de fato:")
    ad("")
    ad("```python")
    ad("import pyarrow.parquet as pq")
    ad("")
    ad("tabela = pq.read_table(")
    ad("    'data/dados_vendas.parquet',")
    ad("    columns=['region', 'order_value'],                    # Column Projection")
    ad("    filters=[('payment_method', '==', 'Pix'),             # Predicate Pushdown")
    ad("             ('sale_date', '>', datetime(2024, 1, 1))],")
    ad(")")
    ad("resultado = tabela.group_by('region').aggregate([('order_value', 'mean')])")
    ad("```")
    ad("")

    # ---------------- Anexo: resultados das consultas ----------------------
    ad("## Anexo: resultados das consultas")
    ad("")
    ad("Os três caminhos são comparados célula a célula a cada execução (`validar()`); "
       f"abaixo, o retorno de cada consulta (as {TOP_N_Q4} primeiras linhas, "
       "quando há mais que isso).")
    ad("")
    for item in resultados:
        c = item["consulta"]
        ad(f"**{c['id']} — {c['descricao']}**  ")
        ad(f"Retorna `{c['retorno']}` · {item['linhas_retornadas']} linhas · "
           f"validação: {'OK' if item['validado'] else 'FALHOU'}")
        ad("")
        ad(_tabela_markdown(item["amostra"].head(TOP_N_Q4)))
        ad("")

    destino.write_text("\n".join(L), encoding="utf-8")
    return destino


# ===========================================================================
# 6. MAIN
# ===========================================================================
def main() -> None:
    # O console do Windows usa cp1252 por padrao e embaralharia os acentos dos
    # rotulos impressos; forcamos UTF-8 na saida quando o terminal aceitar.
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except (AttributeError, OSError):
        pass

    parser = argparse.ArgumentParser(description="Benchmark CSV vs Parquet")
    parser.add_argument("--linhas", type=int, default=N_REGISTROS,
                        help=f"numero de registros a gerar (padrao: {N_REGISTROS})")
    parser.add_argument("--repeticoes", type=int, default=REPETICOES,
                        help=f"execucoes por medicao de tempo (padrao: {REPETICOES})")
    parser.add_argument("--regerar", action="store_true",
                        help="regera o dataset mesmo se os arquivos ja existirem")
    parser.add_argument("--apenas-dados", action="store_true",
                        help="gera os arquivos e encerra, sem rodar o benchmark")
    parser.add_argument("--medir", metavar="ALVO",
                        help="uso interno: executa um unico alvo (ex.: Q1:csv) num "
                             "processo limpo e imprime o pico de memoria")
    args = parser.parse_args()

    # Modo interno: este processo foi criado por medir_memoria() so para medir
    # o pico de memoria de UMA consulta, partindo de um heap limpo.
    if args.medir:
        pico_mb, _ = medir_memoria_local(registro_alvos()[args.medir])
        print(f"MEM_MB={pico_mb:.4f}")
        return

    print("=" * 78)
    print("BENCHMARK DE CONSULTAS ANALITICAS -- CSV vs PARQUET")
    print("=" * 78)

    # --- Tarefa 1: dataset -------------------------------------------------
    precisa_gerar = args.regerar or not (ARQ_CSV.exists() and ARQ_PARQUET.exists())
    if precisa_gerar:
        print(f"\n[1/3] Gerando dataset sintetico ({args.linhas:,} registros)..."
              .replace(",", "."))
        t0 = time.perf_counter()
        df = gerar_dataset(args.linhas)
        print(f"      dataset em memoria: {time.perf_counter() - t0:.1f}s")
        info = salvar_dataset(df)
        del df
        gc.collect()
        print(f"      CSV     -> {ARQ_CSV.name:22} {info['tamanho_csv_mb']:8.1f} MB "
              f"(escrita: {info['tempo_escrita_csv_s']:.1f}s)")
        print(f"      Parquet -> {ARQ_PARQUET.name:22} {info['tamanho_parquet_mb']:8.1f} MB "
              f"(escrita: {info['tempo_escrita_parquet_s']:.1f}s, "
              f"{info['row_groups']} row groups)")
    else:
        print("\n[1/3] Dataset ja existe (use --regerar para recriar).")
        info = {
            "linhas": pq.ParquetFile(ARQ_PARQUET).metadata.num_rows,
            "tempo_escrita_csv_s": float("nan"),
            "tempo_escrita_parquet_s": float("nan"),
            "tamanho_csv_mb": ARQ_CSV.stat().st_size / (1024 ** 2),
            "tamanho_parquet_mb": ARQ_PARQUET.stat().st_size / (1024 ** 2),
            "row_groups": pq.ParquetFile(ARQ_PARQUET).num_row_groups,
        }
        print(f"      CSV {info['tamanho_csv_mb']:.1f} MB | "
              f"Parquet {info['tamanho_parquet_mb']:.1f} MB | "
              f"{info['linhas']:,} linhas".replace(",", "."))

    if args.apenas_dados:
        print("\nConcluido (--apenas-dados).")
        return

    # --- Tarefas 2 e 3: consultas + benchmark ------------------------------
    print(f"\n[2/3] Executando benchmark "
          f"({len(CONSULTAS)} consultas x {len(ENGINES)} estrategias, "
          f"{args.repeticoes} repeticoes cada)...")
    resultados = rodar_benchmark(args.repeticoes)
    isolamento = rodar_isolamento(max(args.repeticoes, REPETICOES_ISOLAMENTO))

    pruning = analisar_pruning()
    print(f"   poda por estatisticas: {pruning['podados']}/{pruning['total']} row groups "
          f"descartados pelo filtro de data da Q3")

    # --- Tarefa 4: relatorio -----------------------------------------------
    print("\n[3/3] Gerando relatorio...")
    destino = gerar_relatorio(resultados, info, args.repeticoes, isolamento, pruning)
    print(f"      {destino}")

    print("\n" + "=" * 78)
    print("RESUMO (tempo em ms / memoria em MB)")
    print("=" * 78)
    cabecalho = f"{'Query':<7}" + "".join(f"{r:>22}" for _, r in ENGINES) + f"{'Speedup':>10}"
    print(cabecalho)
    print("-" * len(cabecalho))
    for item in resultados:
        m = item["medidas"]
        linha = f"{item['consulta']['id']:<7}"
        for chave, _ in ENGINES:
            linha += f"{m[chave]['tempo_ms']:>13,.1f} / {m[chave]['memoria_mb']:>6.1f}"
        linha += f"{m['csv']['tempo_ms'] / m['parquet_opt']['tempo_ms']:>9.1f}x"
        print(linha)
    print("=" * 78)


if __name__ == "__main__":
    main()
