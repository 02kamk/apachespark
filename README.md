# Apache Spark com Delta Lake e Apache Iceberg

![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-3.5.3-E25A1C?logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.2.0-00ADD4)
![Apache Iceberg](https://img.shields.io/badge/Apache%20Iceberg-1.6.1-1E4E79)
![uv](https://img.shields.io/badge/gerenciador-uv-DE5FE9)

📘 **Documentação completa:** https://02kamk.github.io/apachespark/

👥 **Integrantes:** Kamily Hilário das Luzes · Kauã Lucas Alves Barbosa

---

## Sumário

- [Descrição](#descrição)
- [Avisos](#avisos)
- [Pré-requisitos](#pré-requisitos)
- [Instalação](#instalação)
- [Howto — como usar o projeto](#howto--como-usar-o-projeto)
- [Testes](#testes)
- [Documentação](#documentação)
- [Referências](#referências)

---

## Descrição

Trabalho de pesquisa da disciplina de **Engenharia de Dados (SATC)**. O projeto monta um ambiente local com
**PySpark** e **Jupyter** e demonstra, na prática, os comandos `INSERT`, `UPDATE` e `DELETE` — além de `MERGE`,
histórico de versões e *time travel* — em tabelas nos formatos **Delta Lake** e **Apache Iceberg**.

Os dois notebooks usam o **mesmo cenário** (uma academia com as tabelas `planos`, `alunos` e `checkins`) e o
**mesmo roteiro**, para facilitar a comparação entre os dois formatos.

![Modelo ER](docs/assets/modelo-er.png)

### Estrutura do projeto

```text
apachespark/
├── docs/                      # páginas do site MkDocs
│   ├── assets/modelo-er.png   # diagrama ER
│   ├── index.md               # contexto do trabalho
│   ├── spark.md               # Apache Spark (PySpark)
│   ├── delta.md               # Delta Lake
│   └── iceberg.md             # Apache Iceberg
├── notebooks/
│   ├── spark-delta.ipynb      # cenário, DDL, INSERT/UPDATE/DELETE em Delta Lake
│   └── spark-iceberg.ipynb    # cenário, DDL, INSERT/UPDATE/DELETE em Apache Iceberg
├── .gitignore
├── .python-version            # versão do Python usada pelo uv (3.11)
├── mkdocs.yml                 # configuração do site de documentação
├── pyproject.toml             # dependências do projeto
├── uv.lock                    # versões exatas de todas as dependências
└── README.md
```

---

## Avisos

> [!WARNING]
> **Use Linux.** O projeto foi feito e testado no **Ubuntu 24.04** (inclusive dentro do **WSL2** no Windows 11).
> No Windows "puro" o Spark exige configurações extras (winutils/Hadoop) e não é suportado aqui.

> [!IMPORTANT]
> **A primeira execução precisa de internet.** Ao criar a SparkSession, o Spark baixa do Maven Central os pacotes
> `.jar` do Delta Lake e do Iceberg (ficam em cache em `~/.ivy2`). Essa primeira célula pode levar de 1 a 3 minutos.

> [!NOTE]
> - Cada notebook **apaga e recria** a pasta `spark-warehouse/` no início, para sempre rodar do zero.
>   Essa pasta está no `.gitignore` e não vai para o GitHub.
> - Os dados são **fictícios e criados pelo grupo** (nenhum dataset externo é baixado).
> - As versões do PySpark, do Delta e do Iceberg **precisam combinar** (Spark 3.5 / Scala 2.12).
>   Não atualize uma sem conferir a compatibilidade das outras.
> - Projeto **acadêmico**, para estudo — não use como está em produção.

---

## Pré-requisitos

| Requisito | Versão | Observação |
|---|---|---|
| Sistema operacional | Ubuntu 24.04 LTS (nativo ou WSL2) | outras distros Linux devem funcionar |
| Java (OpenJDK) | **17** | obrigatório — o Spark roda na JVM |
| Git | qualquer recente | para clonar o repositório |
| [uv](https://docs.astral.sh/uv/) | 0.8 ou superior | gerenciador de projeto e pacotes Python |
| Python | 3.11 | **instalado automaticamente pelo uv** |
| VS Code + extensões Python e Jupyter | recente | opcional — também dá para usar o JupyterLab |

**Bibliotecas Python** (instaladas pelo `uv sync`, versões travadas no [`uv.lock`](uv.lock)):

| Biblioteca | Versão | Para que serve |
|---|---|---|
| `pyspark` | 3.5.3 | Apache Spark para Python |
| `delta-spark` | 3.2.0 | API Python do Delta Lake (`DeltaTable`) |
| `jupyterlab` | 4.x | interface de notebooks |
| `ipykernel` | 7.x | kernel Python para os notebooks |
| `mkdocs` *(dev)* | 1.6.x | gerador do site de documentação |
| `mkdocs-material` *(dev)* | 9.x | tema do site de documentação |

**Pacotes do Spark** (`.jar`, baixados automaticamente pela SparkSession):

| Pacote | Versão |
|---|---|
| `io.delta:delta-spark_2.12` | 3.2.0 |
| `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12` | 1.6.1 |

---

## Instalação

### 1. Instalar Java 17, Git e utilitários

```bash
sudo apt update
sudo apt install -y git curl unzip openjdk-17-jdk
java -version
```

A saída deve começar com `openjdk version "17`.

<details>
<summary>Tenho outra versão do Java como padrão. Como trocar para a 17?</summary>

```bash
sudo update-alternatives --config java
```

Digite o número da linha que contém `java-17-openjdk` e pressione Enter.

</details>

### 2. Instalar o uv

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
source ~/.bashrc
uv --version
```

### 3. Clonar o repositório

```bash
mkdir -p ~/projetos && cd ~/projetos
git clone https://github.com/02kamk/apachespark.git
cd apachespark
```

> [!TIP]
> No WSL, mantenha o projeto dentro do Linux (`~/projetos`) e não em `/mnt/c/...`: o Spark fica bem mais rápido.

### 4. Criar o ambiente virtual e instalar as dependências

```bash
uv sync
```

Esse único comando lê o `pyproject.toml` e o `uv.lock`, baixa o Python 3.11 (se necessário), cria a pasta `.venv`
e instala exatamente as mesmas versões usadas no projeto.

### 5. Verificar a instalação

```bash
uv run python -c "import pyspark, delta; print('PySpark', pyspark.__version__)"
```

Saída esperada: `PySpark 3.5.3`.

<details>
<summary>Como este ambiente foi criado do zero (apenas para registro)</summary>

```bash
uv init --name spark-delta-iceberg --python 3.11
uv add pyspark==3.5.3 delta-spark==3.2.0 jupyterlab ipykernel
uv add --dev "mkdocs>=1.6.1,<2" "mkdocs-material>=9.7.7,<10"
```

</details>

---

## Howto — como usar o projeto

### Abrir os notebooks

**Opção A — VS Code (recomendado)**

1. Instale as extensões **Python** e **Jupyter** (Microsoft). No WSL, instale também a extensão **WSL** no VS Code do Windows.
2. Na pasta do projeto, rode `code .`
3. Abra `notebooks/spark-delta.ipynb`.
4. Clique em **Select Kernel** → **Python Environments** → **.venv (Python 3.11)**.
5. Clique em **Run All**. Depois repita com `notebooks/spark-iceberg.ipynb`.

**Opção B — JupyterLab**

```bash
uv run jupyter lab
```

Abra o link `http://localhost:8888/lab?token=...` exibido no terminal, entre em `notebooks/` e use
**Run → Run All Cells**.

> [!TIP]
> Enquanto a SparkSession está ativa, o painel do Spark (Spark UI) fica disponível em http://localhost:4040,
> mostrando os jobs executados por baixo de cada comando.

### O que cada notebook faz

| Seção | Etapa | Exemplo do cenário |
|---|---|---|
| 1 | Cenário e modelo ER | academia: `planos`, `alunos`, `checkins` |
| 2 | Ambiente | conferência de versões e criação da SparkSession |
| 3 | DDL | `CREATE TABLE ... USING delta` / `USING iceberg` |
| 4 | INSERT | carga inicial via SQL + 1 aluno via DataFrame |
| 5 | UPDATE | reajuste de 10% no plano Mensal; aluna troca de plano |
| 6 | DELETE | aluno inativo cancela: remove check-ins e cadastro |
| 7 | MERGE | atualiza um aluno existente e insere um novo no mesmo comando |
| 8 | Histórico | `DESCRIBE HISTORY` (Delta) / tabela `.snapshots` (Iceberg) + *time travel* |
| 9 | Arquivos | mostra os Parquet e os metadados gravados no disco |

### Exemplos de código

Criando a SparkSession com **Delta Lake**:

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("SparkDeltaLake")
    .master("local[*]")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
```

Criando a SparkSession com **Apache Iceberg** (catálogo local `local`):

```python
spark = (
    SparkSession.builder
    .appName("SparkIceberg")
    .master("local[*]")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "spark-warehouse/iceberg")
    .getOrCreate()
)
```

`INSERT`, `UPDATE` e `DELETE` — a sintaxe SQL é a mesma nos dois formatos; muda só o nome da tabela
(`academia.planos` no Delta, `local.academia.planos` no Iceberg):

```python
spark.sql("""
INSERT INTO academia.planos VALUES
  (1, 'Mensal',     99.90,  1),
  (2, 'Trimestral', 269.70, 3),
  (3, 'Anual',      899.00, 12)
""")

spark.sql("UPDATE academia.planos SET valor_mensal = ROUND(valor_mensal * 1.10, 2) WHERE nome = 'Mensal'")

spark.sql("DELETE FROM academia.alunos WHERE id_aluno = 4")
```

Resultado do `UPDATE` na tabela `planos`:

| id_plano | nome | valor_mensal (antes) | valor_mensal (depois) |
|---|---|---|---|
| 1 | Mensal | 99.90 | **109.89** |
| 2 | Trimestral | 269.70 | 269.70 |
| 3 | Anual | 899.00 | 899.00 |

*Time travel* — consultar a tabela como ela era antes das alterações:

```python
# Delta Lake: pela versão
spark.sql("SELECT * FROM academia.alunos VERSION AS OF 1").show()

# Apache Iceberg: pelo id do snapshot
spark.sql("SELECT snapshot_id, operation FROM local.academia.alunos.snapshots").show()
spark.sql("SELECT * FROM local.academia.alunos VERSION AS OF <snapshot_id>").show()
```

### Visualizar a documentação localmente

```bash
uv run mkdocs serve
```

Acesse http://127.0.0.1:8000. Para publicar uma nova versão no GitHub Pages:

```bash
uv run mkdocs gh-deploy
```

---

## Testes

O projeto não tem testes unitários: a validação é feita **executando os notebooks de ponta a ponta** e
**gerando a documentação em modo estrito**. Os comandos abaixo rodam tudo pelo terminal, sem abrir o VS Code.

### 1. Teste do ambiente

```bash
uv run python -c "import pyspark, delta; print('PySpark', pyspark.__version__)"
```

✅ Passa se imprimir `PySpark 3.5.3`.

### 2. Execução completa dos notebooks

```bash
uv run jupyter nbconvert --to notebook --execute --inplace notebooks/spark-delta.ipynb
uv run jupyter nbconvert --to notebook --execute --inplace notebooks/spark-iceberg.ipynb
```

✅ Passa se os dois comandos terminarem sem `CellExecutionError`. O `--inplace` salva os resultados dentro dos
próprios notebooks.

Conferências feitas dentro dos notebooks (resultados esperados):

| Verificação | Delta Lake | Apache Iceberg |
|---|---|---|
| Valor do plano Mensal após o UPDATE | 109.89 | 109.89 |
| Alunos após o MERGE | ids 1, 2, 3, 5, 6, 7 | ids 1, 2, 3, 5, 6, 7 |
| Check-ins após o DELETE | 6 (ids 5 e 7 removidos) | 6 (ids 5 e 7 removidos) |
| Histórico da tabela `alunos` | versões 0 a 5 | 5 snapshots |
| *Time travel* para o 1º estado | 5 alunos, com o id 4 | 5 alunos, com o id 4 |

### 3. Build da documentação

```bash
uv run mkdocs build --strict
```

✅ Passa se terminar com `Documentation built` — o modo `--strict` transforma em erro qualquer link quebrado
ou página faltando.

### Problemas comuns

| Erro | Causa | Solução |
|---|---|---|
| `JAVA_HOME is not set` / `Java gateway process exited` | Java não instalado | `sudo apt install -y openjdk-17-jdk` |
| `ModuleNotFoundError: No module named 'pyspark'` | kernel errado no notebook | selecione o kernel `.venv` |
| Célula da SparkSession trava ou falha no download | sem internet para baixar os `.jar` | conecte-se à internet e rode de novo |
| `ClassNotFoundException: ...SessionExtension` | versões incompatíveis | mantenha exatamente as versões deste README |
| `uv: command not found` | terminal aberto antes da instalação | `source ~/.bashrc` ou abra um novo terminal |

---

## Documentação

📘 **https://02kamk.github.io/apachespark/**

A documentação completa (MkDocs Material) explica o **conteúdo** do trabalho — o README explica **como rodar**:

| Página | Conteúdo |
|---|---|
| [Contexto do trabalho](https://02kamk.github.io/apachespark/) | objetivo, cenário, modelo ER, dataset e ambiente |
| [Apache Spark (PySpark)](https://02kamk.github.io/apachespark/spark/) | conceitos do Spark e integração com os formatos de tabela |
| [Delta Lake](https://02kamk.github.io/apachespark/delta/) | `_delta_log`, DDL, INSERT, UPDATE, DELETE, MERGE e *time travel* |
| [Apache Iceberg](https://02kamk.github.io/apachespark/iceberg/) | catálogos, snapshots, particionamento oculto e comparação com o Delta |

O código-fonte das páginas está na pasta [`docs/`](docs/).

---

## Referências

- [Apache Spark 3.5.3 — Documentação](https://spark.apache.org/docs/3.5.3/)
- [Delta Lake — Quickstart](https://docs.delta.io/latest/quick-start.html) · [Releases e compatibilidade](https://docs.delta.io/latest/releases.html)
- [Apache Iceberg — Spark Quickstart](https://iceberg.apache.org/spark-quickstart/) · [Maven: iceberg-spark-runtime-3.5](https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-spark-runtime-3.5)
- [uv — Documentação](https://docs.astral.sh/uv/)
- [MkDocs Material](https://squidfunk.github.io/mkdocs-material/)
- Projetos de exemplo do professor: [jlsilva01/spark-delta](https://github.com/jlsilva01/spark-delta) · [jlsilva01/spark-iceberg](https://github.com/jlsilva01/spark-iceberg)
- [Canal DataWay BR](https://www.youtube.com/@datawaybr)
