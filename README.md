# Apache Spark com Delta Lake e Apache Iceberg

Trabalho de pesquisa da disciplina de **Engenharia de Dados (SATC)**: ambiente local com **PySpark** e **Jupyter**
demonstrando os comandos `INSERT`, `UPDATE` e `DELETE` (e também `MERGE` e *time travel*) em tabelas no formato
**Delta Lake** e **Apache Iceberg**.

📘 **Documentação (MkDocs):** https://02kamk.github.io/apachespark/

👥 **Integrantes:** Kamily Hilário das Luzes · Kauã Lucas Alves Barbosa 

---

## Sumário

- [Estrutura do projeto](#estrutura-do-projeto)
- [Tecnologias e versões](#tecnologias-e-versões)
- [Pré-requisitos](#pré-requisitos)
- [Passo a passo para reproduzir o ambiente](#passo-a-passo-para-reproduzir-o-ambiente)
- [Executando os notebooks](#executando-os-notebooks)
- [Documentação com MkDocs](#documentação-com-mkdocs)
- [Como o ambiente foi criado do zero](#como-o-ambiente-foi-criado-do-zero)
- [Problemas comuns](#problemas-comuns)
- [Referências](#referências)

---

## Estrutura do projeto

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
├── pyproject.toml             # dependências do projeto (gerenciado pelo uv)
├── uv.lock                    # versões exatas de todas as dependências
└── README.md
```

> As tabelas são gravadas em `spark-warehouse/` quando os notebooks rodam. Essa pasta está no `.gitignore`
> (não vai para o GitHub) e é recriada do zero a cada execução.

## Tecnologias e versões

| Ferramenta | Versão | Para que serve |
|---|---|---|
| Ubuntu | 24.04 LTS | sistema operacional (também funciona no WSL2 do Windows 11) |
| OpenJDK | 17 | o Spark roda na JVM — **obrigatório** |
| [uv](https://docs.astral.sh/uv/) | 0.8+ | gerenciador de projeto, de pacotes e da versão do Python |
| Python | 3.11 | instalado automaticamente pelo uv |
| [PySpark](https://spark.apache.org/docs/3.5.3/api/python/) | 3.5.3 | Apache Spark para Python |
| [delta-spark](https://docs.delta.io/latest/quick-start.html) | 3.2.0 | API Python do Delta Lake (o `.jar` é baixado pelo Spark) |
| [iceberg-spark-runtime-3.5_2.12](https://iceberg.apache.org/spark-quickstart/) | 1.6.1 | `.jar` do Iceberg (baixado pelo Spark, não é pacote Python) |
| JupyterLab / ipykernel | 4.x / 7.x | execução dos notebooks |
| MkDocs / mkdocs-material | 1.6 / 9.x | site de documentação (dependências de desenvolvimento) |

As versões exatas de todas as bibliotecas estão travadas no [`uv.lock`](uv.lock).

## Pré-requisitos

- Ubuntu 24.04 (ou WSL2 com Ubuntu) com acesso à internet
- Conta no GitHub (apenas para publicar a documentação)

> ⚠️ Não use Windows "puro": o Spark precisa de configurações extras (winutils/Hadoop) para funcionar nele.

## Passo a passo para reproduzir o ambiente

### 1. Atualizar o sistema e instalar Git, curl e Java 17

```bash
sudo apt update
sudo apt install -y git curl openjdk-17-jdk
```

Confira se o Java foi instalado:

```bash
java -version
```

A saída deve começar com `openjdk version "17...`.

<details>
<summary>Tenho mais de uma versão do Java instalada. Como escolher a 17?</summary>

```bash
sudo update-alternatives --config java
```

Digite o número correspondente ao `java-17-openjdk` e pressione Enter. Opcionalmente, defina o `JAVA_HOME`:

```bash
echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> ~/.bashrc
source ~/.bashrc
```

</details>

### 2. Instalar o uv

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
source ~/.bashrc
uv --version
```

### 3. Clonar o repositório

```bash
git clone https://github.com/02kamk/apachespark.git
cd apachespark
```

### 4. Criar o ambiente virtual e instalar as dependências

Um único comando lê o `pyproject.toml` e o `uv.lock`, baixa o Python 3.11 (se necessário), cria a pasta `.venv`
e instala exatamente as mesmas versões usadas no projeto:

```bash
uv sync
```

Ative o ambiente virtual:

```bash
source .venv/bin/activate
```

Teste rápido:

```bash
python -c "import pyspark, delta; print('PySpark', pyspark.__version__)"
```

## Executando os notebooks

Os notebooks rodam do início ao fim com **Run All** — a primeira célula de cada um apaga o `spark-warehouse`
para garantir uma execução limpa.

> 🌐 **Na primeira execução** o Spark baixa os `.jar` do Delta e do Iceberg do Maven Central (fica em `~/.ivy2`).
> Precisa de internet e pode levar alguns minutos. Depois disso fica em cache.

### Opção A — VS Code (recomendado)

1. Instale o [VS Code](https://code.visualstudio.com/) e as extensões **Python** e **Jupyter** (ambas da Microsoft).
2. Abra a pasta do projeto:
   ```bash
   code .
   ```
3. Abra `notebooks/spark-delta.ipynb`.
4. Clique em **Select Kernel** (canto superior direito) → **Python Environments** → escolha **`.venv (Python 3.11)`**.
5. Clique em **Run All**.
6. Repita com `notebooks/spark-iceberg.ipynb`.

### Opção B — JupyterLab

```bash
uv run jupyter lab
```

O navegador abre automaticamente. Entre na pasta `notebooks/`, abra o notebook desejado e use
**Run → Run All Cells**. O kernel `Python 3` do JupyterLab já é o do `.venv`, pois ele foi iniciado com `uv run`.

## Documentação com MkDocs

O conteúdo do trabalho (contexto, Spark, Delta e Iceberg) está em `docs/` e é publicado com
[MkDocs Material](https://squidfunk.github.io/mkdocs-material/).

### Visualizar localmente

```bash
uv run mkdocs serve
```

Acesse http://127.0.0.1:8000 no navegador. O site recarrega sozinho quando um arquivo de `docs/` é alterado.

### Publicar no GitHub Pages

```bash
uv run mkdocs gh-deploy
```

O comando gera o site e o envia para a branch `gh-pages` do repositório. Na primeira publicação, no GitHub:
**Settings → Pages → Build and deployment → Source: _Deploy from a branch_ → Branch: `gh-pages` / `(root)` → Save**.
Em 1–2 minutos o site fica disponível em `https://02kamk.github.io/apachespark/`.

## Como o ambiente foi criado do zero

Para registro, estes foram os comandos usados para criar o projeto (não é preciso repeti-los para reproduzir —
basta o `uv sync` acima):

```bash
uv init --name spark-delta-iceberg --python 3.11
uv add pyspark==3.5.3 delta-spark==3.2.0 jupyterlab ipykernel
uv add --dev mkdocs mkdocs-material
```

## Problemas comuns

| Erro | Causa | Solução |
|---|---|---|
| `JAVA_HOME is not set` / `Java gateway process exited before sending its port number` | Java não instalado | `sudo apt install -y openjdk-17-jdk` |
| `UnsupportedClassVersionError` ou erros estranhos de JVM | Java muito antigo ou muito novo | use o Java 17 (`sudo update-alternatives --config java`) |
| `ModuleNotFoundError: No module named 'pyspark'` | kernel errado no notebook | selecione o kernel `.venv` |
| Demora longa ou erro de download na 1ª célula do Spark | sem internet para baixar os `.jar` | conecte-se à internet e rode de novo |
| `ClassNotFoundException: ...DeltaSparkSessionExtension` / `...IcebergSparkSessionExtensions` | versão do `.jar` incompatível | mantenha exatamente as versões do README |
| `uv: command not found` | terminal aberto antes da instalação | `source ~/.bashrc` ou abra um novo terminal |
| `mkdocs gh-deploy` pede usuário/senha | GitHub não aceita senha | use um [Personal Access Token](https://github.com/settings/tokens) ou SSH |

## Referências

- [Apache Spark 3.5.3 — Documentação](https://spark.apache.org/docs/3.5.3/)
- [Delta Lake — Quickstart](https://docs.delta.io/latest/quick-start.html) · [Releases/compatibilidade](https://docs.delta.io/latest/releases.html)
- [Apache Iceberg — Spark Quickstart](https://iceberg.apache.org/spark-quickstart/) · [Maven iceberg-spark-runtime-3.5](https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-spark-runtime-3.5)
- [uv — Documentação](https://docs.astral.sh/uv/)
- [MkDocs Material](https://squidfunk.github.io/mkdocs-material/)
- Projetos de exemplo: [jlsilva01/spark-delta](https://github.com/jlsilva01/spark-delta) · [jlsilva01/spark-iceberg](https://github.com/jlsilva01/spark-iceberg)
- [Canal DataWay BR](https://www.youtube.com/@datawaybr)
