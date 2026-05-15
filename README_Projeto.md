# Pipeline de Dados Legislativos Brasileiros - Documentação do Projeto

## 📋 Visão Geral

Pipeline automatizado de ingestão de dados da API de Dados Abertos da Câmara dos Deputados, implementado em Databricks com arquitetura Medallion (Bronze/Silver/Gold).

---

## 🏗️ Arquitetura

### Padrão Medallion Architecture
```
┌─────────────────────────────────────────────────────────┐
│         API Câmara dos Deputados (REST)                 │
│         https://dadosabertos.camara.leg.br              │
└────────────────────┬────────────────────────────────────┘
                     │ HTTP + Retry Logic (5 tentativas)
                     ↓
┌─────────────────────────────────────────────────────────┐
│  BaseBronzeIngestion (Template Method Pattern)          │
│  ├─ _fetch_standard: paginação automática               │
│  ├─ _fetch_multithreaded: paralelização (ThreadPool)    │
│  └─ _fetch_with_fallback: download de arquivos          │
└────────────────────┬────────────────────────────────────┘
                     │ Python Nativo (os, open, json)
                     ↓
┌─────────────────────────────────────────────────────────┐
│  Unity Catalog Volumes (Landing Zone - POSIX/FUSE)      │
│  /Volumes/workspace/camara_bronze/landing_zone/          │
│  • JSON raw preservado para auditoria                    │
│  • Timestamp em cada arquivo                             │
└────────────────────┬────────────────────────────────────┘
                     │ PySpark DataFrame API
                     ↓
┌─────────────────────────────────────────────────────────┐
│  Delta Lake Tables (Bronze Layer - ACID)                │
│  workspace.camara_bronze.{entidade}                      │
│  • Schema enforcement automático                         │
│  • Time travel habilitado                                │
│  • Modo overwrite para idempotência                      │
└─────────────────────────────────────────────────────────┘
```

---

## 🎯 Design Patterns Implementados

### 1. Template Method Pattern
**Onde:** `BaseBronzeIngestion`

**Por quê:** Define o esqueleto do algoritmo de ingestão, permitindo que subclasses implementem apenas o método `fetch_data()`.

```python
# Template invariável (base class)
def execute(self):
    raw_data = self.fetch_data()  # ← Implementado pela child class
    if raw_data:
        self.save(raw_data)

# Child class implementa apenas fetch_data
class DeputadosIngestion(BaseBronzeIngestion):
    def fetch_data(self):
        return self._fetch_standard(params=...)
```

**Benefícios:**
- Zero duplicação de código
- Workflow consistente entre todas as entidades
- Fácil adicionar novas entidades (10-15 linhas)

### 2. Strategy Pattern (Implicit)
**Onde:** Três engines de extração

1. **_fetch_standard**: Paginação simples (deputados, frentes)
2. **_fetch_multithreaded**: Paralelização com ThreadPool (despesas por deputado, membros de órgãos)
3. **_fetch_with_fallback**: Download de arquivos quando API falha

**Parâmetro `paginated`:**
- `paginated=True` (padrão): Adiciona `formato='json'` e `pagina` aos requests
- `paginated=False`: Faz request **sem parâmetros** (usado em endpoints individuais como `/orgaos/{id}/membros` e `/eventos/{id}/deputados` que não aceitam paginação)

**Por quê:** Diferentes entidades requerem estratégias diferentes de fetch, mas todas compartilham o mesmo fluxo de armazenamento.

### 3. Convention over Configuration
**Onde:** Estrutura de diretórios e nomenclatura

```
configs/
  ├── generic.json          ← Sempre no mesmo lugar
  └── {entity_name}.json    ← Pattern padronizado

notebooks/
  └── 01_bronze/
      └── 01_ingestion_{entity}
```

**Benefícios:**
- Paths construídos automaticamente: `configs/{entity_name}.json`
- Novos devs sabem onde procurar
- Reduz erros de configuração

---

## 🔧 Decisões Técnicas Principais

### 1. Python Nativo vs dbutils.fs
**Decisão:** Python nativo (`os`, `open()`)

**Razão:**
- Unity Catalog Volumes usa FUSE/POSIX
- Testável localmente sem Databricks
- Consistente com PySpark (Python-first)
- Zero overhead de API translation

**Trade-off:** Limitado a UC Volumes (mas é a best practice)

### 2. Sem dbutils na Assinatura
**Antes:**
```python
def __init__(self, spark, dbutils, entity_name)
```

**Depois:**
```python
def __init__(self, spark, entity_name)
```

**Benefícios:**
- 33% menos parâmetros
- Código roda fora de notebooks
- Mais fácil de testar

### 3. Configuração JSON Separada
**Estrutura:**
```json
// generic.json - compartilhado
{
  "base_url": "https://dadosabertos.camara.leg.br/api/v2",
  "catalog": "workspace",
  "layer": "bronze",
  "idLegislatura": [57]
}

// deputados.json - específico
{
  "entity": "deputados",
  "params": {"ordem": "ASC", "itens": 100},
  "file_url": "https://.../deputados.json"  // fallback
}
```

**Por quê:**
- Mudanças sem tocar em código
- Fácil version control (Git)
- Equipe de produto pode ajustar params

### 4. Fallback Automático
**Fluxo:**
```
API Request → 5 Retries → Falha? → Download file_url → Parse → Return
```

**Configuração:**
```json
{
  "file_url": "https://.../despesas-{ano}.zip",
  "params": {"ano": [2024]}
}
```

Sistema substitui `{ano}` → `2024` automaticamente, baixa ZIP, extrai, retorna dados.

**Resiliência:**
- API instável → usa arquivo bulk
- Detecta ZIP automaticamente (`.endswith('.zip')`)
- Parse JSON robusto (trata diferentes estruturas)

---

## 📊 Métricas do Projeto

### Código
- **3 parâmetros → 2** (redução de 33%)
- **8 entidades configuradas** via JSON
- **3 engines de extração** (standard, multithreaded, fallback)
- **0 linhas duplicadas** (DRY principle)
- **859 registros** de deputados ingeridos com sucesso

### Performance
- **ThreadPool paralelização**: 10 workers simultâneos
- **Retry exponential backoff**: 0s, 2s, 4s, 8s, 16s
- **Custo estimado**: < $1 USD/mês (serverless)

### Testabilidade
- **100% testável localmente** (Python nativo)
- **Mocks simples** (não requer Databricks Connect)
- **CI/CD ready** (roda em GitHub Actions)

---

## 📦 Estrutura do Projeto

```
databricks-data-engineering-br-legislative-pipeline/
│
├── configs/                      # Configurações JSON
│   ├── generic.json             # Config compartilhado
│   ├── deputados.json           # Entidade: deputados
│   ├── despesas.json            # Entidade: despesas
│   ├── frentes.json             # Entidade: frentes parlamentares
│   ├── proposicoes.json         # Entidade: proposições
│   ├── votacoes.json            # Entidade: votações
│   └── ...
│
├── modules/                      # Lógica de negócio
│   └── base_ingestion.py        # Template Method + Engines
│
├── utils/                        # Utilitários
│   ├── camara_api.py            # Cliente HTTP + retry
│   ├── storage.py               # Persistência (Landing + Delta)
│   ├── model.py                 # Data classes (IngestionConfig)
│   ├── file_handler.py          # Download ZIP/JSON
│   └── path_utils.py            # Path resolution
│
├── notebooks/                    # Notebooks Databricks
│   └── 01_bronze/
│       ├── 01_ingestion_deputies
│       ├── 02_ingestion_despesas
│       └── ...
│
└── docs/                         # Documentação
    ├── ADR_Python_Nativo_vs_dbutils.md
    └── README_Projeto.md (este arquivo)
```

---

## 🚀 Como Adicionar Nova Entidade

### Exemplo 1: Endpoint Paginado (Padrão)
```python
# notebooks/01_bronze/01_ingestion_deputados

class DeputadosIngestion(BaseBronzeIngestion):
    def __init__(self, spark):
        super().__init__(spark, entity_name='deputados')
    
    def fetch_data(self):
        return self._fetch_standard(params=self.entity_config['params'])

ingestion = DeputadosIngestion(spark)
ingestion.execute()
```

### Exemplo 2: Endpoint Individual (Sem Paginação)
```python
# notebooks/01_bronze/12_ingestion_orgaos_eventos

class OrgaosEventosIngestion(BaseBronzeIngestion):
    def __init__(self, spark):
        super().__init__(spark, entity_name='orgaos_eventos')
    
    def fetch_data(self):
        return self._fetch_multithreaded(
            source_table=f"{self.generic_config['catalog']}.camara_bronze.orgaos",
            id_column='id',
            endpoint_builder=lambda id: f"orgaos/{id}/eventos",
            foreign_key='id_orgao',
            params=self.entity_config['params'],
            paginated=False  # ← Endpoints individuais não aceitam paginação
        )

ingestion = OrgaosEventosIngestion(spark)
ingestion.execute()
```

**Pronto!** 10-15 linhas de código, nova entidade funcionando.

---

## 🎓 Próximos Passos (Roadmap)

### Fase 2: Silver Layer
- [ ] Limpeza de dados (null handling, deduplicação)
- [ ] Joins entre entidades (deputados + despesas)
- [ ] Transformações de negócio (agregações, métricas)
- [ ] Migrar para Lakeflow Spark Declarative Pipelines (DLT)

### Fase 3: Gold Layer
- [ ] Tabelas agregadas (KPIs, dashboards)
- [ ] Data marts por domínio (gastos, votações, projetos)
- [ ] Materialização de métricas complexas

### Fase 4: Consumo
- [ ] Dashboards Lakeview (BI self-service)
- [ ] APIs REST (FastAPI + MLflow serving)
- [ ] Jobs agendados (atualização diária)

---

## 📚 Referências

### Databricks
- [Unity Catalog Volumes](https://docs.databricks.com/volumes/)
- [Delta Lake](https://docs.databricks.com/delta/)
- [Lakeflow](https://docs.databricks.com/lakeflow/)

### API Câmara dos Deputados
- [Documentação Oficial](https://dadosabertos.camara.leg.br/swagger/api.html)
- [Portal Dados Abertos](https://dadosabertos.camara.leg.br/)

### Design Patterns
- [Template Method Pattern](https://refactoring.guru/design-patterns/template-method)
- [Strategy Pattern](https://refactoring.guru/design-patterns/strategy)

---

## 👥 Contato

**Projeto:** Pipeline de Dados Legislativos Brasileiros  
**Tecnologias:** Databricks, PySpark, Delta Lake, Unity Catalog  
**Padrão:** Medallion Architecture (Bronze/Silver/Gold)  
**Status:** ✅ Bronze Layer implementada e funcional  
**Contato:** emanuelsous@gmail.com
