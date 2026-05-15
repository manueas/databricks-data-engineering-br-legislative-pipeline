# Registro de Decisão de Arquitetura (ADR): Python Nativo vs dbutils.fs

## Contexto
O Pipeline de Dados Legislativos Brasileiros precisa persistir dados da API da Câmara em Unity Catalog Volumes para a camada Bronze.

## Decisão
**Usar operações de arquivo nativas do Python (`os`, `open()`) ao invés de `dbutils.fs`**

## Status
✅ Aceito e Implementado

---

## Justificativa

### 1. Arquitetura do Unity Catalog Volumes
Unity Catalog Volumes implementa **sistema de arquivos POSIX** via **FUSE (Filesystem in Userspace)**:
- Volumes são montados como paths locais: `/Volumes/catalog/schema/volume/`
- Python pode acessá-los diretamente como qualquer filesystem
- Não há necessidade de APIs específicas do Databricks

**Fonte:** [Documentação Databricks - Trabalhar com arquivos em Unity Catalog volumes]
> "Você também pode usar pacotes OSS para comandos utilitários de arquivo, como o módulo Python os"

### 2. Consistência com o Ecossistema PySpark
- **Spark é Python-first**: PySpark é a API principal
- **DataFrames read/write**: Já usando `spark.read.table()` e `df.write.saveAsTable()`
- **Pythônico**: Segue idiomas e convenções do Python

### 3. Testabilidade & Velocidade de Desenvolvimento
| Aspecto | Python Nativo | dbutils.fs |
|---------|---------------|------------|
| **Testes Unitários** | ✅ Rodam localmente com mocks | ❌ Requer ambiente Databricks |
| **Suporte IDE** | ✅ Autocomplete completo | ⚠️ Limitado |
| **Debugging** | ✅ Debugger Python padrão | ⚠️ Apenas em notebooks |
| **CI/CD** | ✅ Compatível GitHub Actions | ❌ Precisa Databricks Connect |

### 4. Manutenibilidade do Código
```python
# Python Nativo (Universal)
import os
os.makedirs(path, exist_ok=True)
with open(file_path, 'w') as f:
    json.dump(data, f)

# dbutils.fs (Específico Databricks)
dbutils.fs.mkdirs(path)
dbutils.fs.put(file_path, json.dumps(data), overwrite=True)
```

**Tempo de Onboarding:**
- Python nativo: ~0 minutos (qualquer dev Python já conhece)
- dbutils.fs: ~1-2 horas (requer treinamento Databricks)

### 5. Mitigação de Vendor Lock-in
Embora estejamos comprometidos com Databricks, usar Python padrão:
- **Reduz acoplamento** com APIs específicas do Databricks
- **Habilita portabilidade** (código roda em qualquer lugar com PySpark)
- **Preparado para o futuro** contra mudanças de API

---

## Implementação

### Antes (Acoplado ao Databricks)
```python
class BaseBronzeIngestion:
    def __init__(self, spark, dbutils, entity_name):
        self.spark = spark
        self.dbutils = dbutils  # ← Dependência Databricks

def save_to_volume(config, data, spark, dbutils):
    dbutils.fs.mkdirs(config.landing_path)  # ← API Databricks
    with open(file_path, 'w') as f:
        json.dump(data, f)
```

### Depois (Python Nativo)
```python
import os

class BaseBronzeIngestion:
    def __init__(self, spark, entity_name):
        self.spark = spark  # ← Apenas Spark necessário

def save_to_volume(config, data, spark):
    os.makedirs(config.landing_path, exist_ok=True)  # ← Python stdlib
    with open(file_path, 'w') as f:
        json.dump(data, f)
```

---

## Consequências

### Positivas
✅ **Testabilidade**: Pode escrever testes unitários que rodam localmente  
✅ **Simplicidade**: 2 parâmetros em vez de 3 (`spark, entity_name` vs `spark, dbutils, entity_name`)  
✅ **Performance**: Acesso POSIX direto, sem camada de tradução de API  
✅ **Padrões**: Segue práticas da comunidade Python/Spark  
✅ **Portabilidade**: Código roda em qualquer ambiente PySpark  

### Negativas
⚠️ **Limitado a UC Volumes**: Se precisarmos de acesso direto a cloud storage (S3/Azure), seria necessário refatoração  
⚠️ **DBFS Root não suportado**: Paths antigos DBFS (`/dbfs/...`) requerem dbutils  

### Mitigação
Se requisitos futuros precisarem de acesso direto à cloud, podemos:
1. **Preferencial**: Continuar usando Unity Catalog Volumes (abstrai storage)
2. **Alternativa**: Implementar interface `StorageProvider` com múltiplos backends

---

## Alternativa Considerada

### Opção: dbutils.fs em Todo Lugar
**Prós:**
- Funciona com qualquer storage Databricks (DBFS, S3, Azure)
- API única para todas as operações de arquivo

**Contras:**
- Requer dbutils em todas as camadas (viola separação de responsabilidades)
- Não testável fora do Databricks
- Inconsistente com padrões PySpark
- Curva de aprendizado mais íngreme para novos membros da equipe

**Conclusão:** Rejeitada porque Unity Catalog Volumes + Python nativo alcança os mesmos objetivos com melhor DX (Developer Experience).

---

## Referências
- [Databricks Docs: Trabalhar com arquivos em Unity Catalog volumes](https://docs.databricks.com/volumes/volume-files/)
- [Arquitetura Unity Catalog Volumes](https://docs.databricks.com/volumes/)
- PEP 8 - Guia de Estilo Python
- Clean Architecture (Robert C. Martin) - Regra de Dependência

---

## Tomadores de Decisão
- Arquitetura: Equipe de Engenharia de Dados
- Revisado: 2026-05-12
- Status: ✅ Implementado
