# Delta Lake

## O que é

O **Delta Lake** é um formato de tabela aberto, criado pela Databricks e hoje mantido pela Linux Foundation.
A ideia é simples: os dados continuam em arquivos **Parquet**, mas ao lado deles fica uma pasta chamada
**`_delta_log/`** — um **diário** em que cada alteração na tabela vira um arquivo JSON numerado.

Pense em um caderno de contas da casa: você não apaga o que escreveu, só acrescenta uma linha nova dizendo
"corrigir o valor da luz para R$ 180". Lendo o caderno do começo ao fim, você sabe o estado atual — e também
consegue saber como estava em qualquer dia anterior. O `_delta_log` funciona assim.

```text
spark-warehouse/delta/academia.db/alunos/
├── _delta_log/
│   ├── 00000000000000000000.json   ← versão 0: CREATE TABLE
│   ├── 00000000000000000001.json   ← versão 1: INSERT
│   ├── ...
│   └── 00000000000000000005.json   ← versão 5: MERGE
├── part-00000-....snappy.parquet   ← dados
└── part-00001-....snappy.parquet
```

**Principais recursos:** transações ACID, `UPDATE` / `DELETE` / `MERGE`, histórico de versões e *time travel*,
evolução e validação de esquema.

## Configuração no Spark

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("SparkDeltaLake")
    .master("local[*]")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.warehouse.dir", "spark-warehouse/delta")
    .getOrCreate()
)
```

A biblioteca Python `delta-spark==3.2.0` (instalada pelo uv) traz a API `DeltaTable`; o `.jar` com a mesma versão
é baixado pelo `spark.jars.packages`. As duas versões **precisam ser iguais**.

## DDL

```sql
CREATE DATABASE IF NOT EXISTS academia;

CREATE TABLE IF NOT EXISTS academia.planos (
    id_plano      INT,
    nome          STRING,
    valor_mensal  DECIMAL(10,2),
    duracao_meses INT
) USING delta;

CREATE TABLE IF NOT EXISTS academia.alunos (
    id_aluno       INT,
    nome           STRING,
    email          STRING,
    cidade         STRING,
    id_plano       INT,
    data_matricula DATE,
    ativo          BOOLEAN
) USING delta;

CREATE TABLE IF NOT EXISTS academia.checkins (
    id_checkin INT,
    id_aluno   INT,
    data_hora  TIMESTAMP,
    unidade    STRING
) USING delta
PARTITIONED BY (unidade);
```

`USING delta` é o que define o formato. Em `checkins`, o `PARTITIONED BY (unidade)` cria uma subpasta por unidade
(`unidade=Centro/`, `unidade=Próspera/`), o que acelera consultas que filtram por unidade.

## INSERT

=== "SQL"

    ```python
    spark.sql("""
    INSERT INTO academia.alunos VALUES
      (1, 'Ana Souza',     'ana@email.com',   'Criciúma',     1, DATE '2024-01-10', true),
      (2, 'Bruno Lima',    'bruno@email.com', 'Içara',        3, DATE '2024-02-05', true),
      (3, 'Carla Mendes',  'carla@email.com', 'Criciúma',     2, DATE '2024-03-12', true),
      (4, 'Diego Rocha',   'diego@email.com', 'Forquilhinha', 1, DATE '2024-04-20', false),
      (5, 'Elisa Martins', 'elisa@email.com', 'Criciúma',     3, DATE '2024-05-02', true)
    """)
    ```

=== "DataFrame (PySpark)"

    ```python
    from datetime import date

    novo_aluno = spark.createDataFrame(
        [(6, "Felipe Costa", "felipe@email.com", "Criciúma", 2, date(2024, 6, 10), True)],
        schema="id_aluno INT, nome STRING, email STRING, cidade STRING, "
               "id_plano INT, data_matricula DATE, ativo BOOLEAN",
    )
    novo_aluno.write.format("delta").mode("append").saveAsTable("academia.alunos")
    ```

Cada `INSERT` gera **novos arquivos Parquet** e **uma nova versão** no `_delta_log`. Os arquivos antigos não são tocados.

## UPDATE

Reajuste de 10% no plano Mensal:

```python
spark.sql("""
UPDATE academia.planos
SET valor_mensal = ROUND(valor_mensal * 1.10, 2)
WHERE nome = 'Mensal'
""")
```

| id_plano | nome | valor_mensal (antes) | valor_mensal (depois) |
|---|---|---|---|
| 1 | Mensal | 99.90 | **109.89** |
| 2 | Trimestral | 269.70 | 269.70 |
| 3 | Anual | 899.00 | 899.00 |

A aluna Ana troca para o plano Anual:

```python
spark.sql("UPDATE academia.alunos SET id_plano = 3 WHERE id_aluno = 1")
```

!!! info "O que acontece por baixo"
    Parquet é imutável — não dá para alterar um arquivo. O Delta então **reescreve só os arquivos que contêm as
    linhas afetadas** e registra no log: "o arquivo antigo X saiu (*remove*), o arquivo novo Y entrou (*add*)".
    O arquivo antigo continua no disco, e é isso que permite o *time travel*. (Ele só é apagado de verdade
    com o comando `VACUUM`.)

## DELETE

O aluno Diego (id 4) cancelou. Primeiro removemos os check-ins dele, depois o cadastro:

```python
spark.sql("DELETE FROM academia.checkins WHERE id_aluno = 4")
spark.sql("DELETE FROM academia.alunos   WHERE id_aluno = 4")
```

Os check-ins 5 e 7 (os dois do Diego) somem; a tabela `checkins` fica com 6 linhas.
No Delta, `UPDATE`, `DELETE` e `MERGE` retornam um DataFrame com a quantidade de linhas afetadas
(`num_affected_rows`), útil para conferir.

## MERGE (upsert)

Chega um lote com a Carla (que mudou de cidade) e a Gabriela (aluna nova). Em um único comando, quem existe é
atualizado e quem não existe é inserido:

```python
spark.sql("""
MERGE INTO academia.alunos AS destino
USING atualizacoes_alunos AS origem
ON destino.id_aluno = origem.id_aluno
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
```

Estado final da tabela `alunos`:

| id_aluno | nome | cidade | id_plano | O que aconteceu |
|---|---|---|---|---|
| 1 | Ana Souza | Criciúma | **3** | UPDATE (trocou de plano) |
| 2 | Bruno Lima | Içara | 3 | — |
| 3 | Carla Mendes | **Içara** | 2 | MERGE → atualizada |
| 5 | Elisa Martins | Criciúma | 3 | — |
| 6 | Felipe Costa | Criciúma | 2 | INSERT via DataFrame |
| 7 | Gabriela Alves | Criciúma | 1 | MERGE → inserida |

## Histórico e *time travel*

```python
spark.sql("DESCRIBE HISTORY academia.alunos") \
    .select("version", "operation") \
    .orderBy("version").show()
```

| version | operation |
|---|---|
| 0 | CREATE TABLE |
| 1 | WRITE (INSERT SQL) |
| 2 | WRITE (append do DataFrame) |
| 3 | UPDATE |
| 4 | DELETE |
| 5 | MERGE |

Consultar a tabela como ela era na **versão 1** (com o Diego ainda lá e a Ana no plano Mensal):

```python
spark.sql("SELECT * FROM academia.alunos VERSION AS OF 1").show()
```

Também é possível pela API Python:

```python
from delta.tables import DeltaTable

DeltaTable.forName(spark, "academia.alunos").history().show()
```

## Referências

- [Delta Lake — Quickstart](https://docs.delta.io/latest/quick-start.html)
- [Delta Lake — Table deletes, updates, and merges](https://docs.delta.io/latest/delta-update.html)
- [Delta Lake — Releases e compatibilidade com Spark](https://docs.delta.io/latest/releases.html)
- [Projeto de exemplo do professor: jlsilva01/spark-delta](https://github.com/jlsilva01/spark-delta)
- [DataWay BR — Como sair do zero no Delta Lake](https://datawaybr.medium.com/como-sair-do-zero-no-delta-lake-em-apenas-uma-aula-d152688a4cc8)
