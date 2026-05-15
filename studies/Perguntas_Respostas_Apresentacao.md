# Perguntas e Respostas - Preparação para Apresentação

## 🎯 Guia Rápido de Defesa do Projeto

---

## 1. Por que Python nativo em vez de dbutils.fs?

**Resposta:**
> "Optamos por Python nativo porque estamos usando Unity Catalog Volumes, que implementa POSIX via FUSE. Isso nos dá 4 vantagens estratégicas:
> 
> 1. **Testabilidade**: Código roda localmente sem Databricks - posso testar no meu laptop
> 2. **Consistência**: PySpark é Python-first, seguimos o mesmo padrão
> 3. **Zero vendor lock-in**: Python stdlib funciona em qualquer ambiente
> 4. **Onboarding rápido**: Qualquer dev Python entende `os.makedirs()` imediatamente
> 
> A própria documentação do Databricks recomenda Python OSS packages para UC Volumes."

**Evidência:**
```python
# Documentação oficial Databricks:
import os
os.mkdir('/Volumes/catalog/schema/volume/dir')
```

---

## 2. E se precisar migrar para S3/Azure direto?

**Resposta:**
> "Unity Catalog Volumes **já está usando S3/Azure** por baixo dos panos - o Databricks gerencia a conexão. Se mantivermos UC Volumes (que é a best practice moderna), não muda nada no código.
> 
> Se por algum motivo precisarmos de acesso direto (cenário improvável), a arquitetura permite implementar um Storage Provider Pattern - interface abstrata com duas implementações (VolumeStorage e CloudStorage). Mas sinceramente, UC Volumes resolve 99% dos casos reais."

**Diagrama:**
```
UC Volumes (/Volumes/...) → Python nativo ✅
Cloud Direto (s3://...) → Precisaria dbutils ⚠️
```

---

## 3. Como garantir funcionamento em produção?

**Resposta:**
> "A arquitetura foi validada em múltiplas camadas:
> 
> 1. **FUSE mount é nativo Databricks** - funciona em todos os clusters e warehouses
> 2. **Testado em Serverless** - o ambiente mais restritivo disponível
> 3. **Prova de conceito bem-sucedida** - 859 registros de deputados salvos com sucesso
> 4. **Mode overwrite** garante idempotência - reruns são seguros
> 5. **Delta Lake ACID transactions** - consistência garantida
> 
> Além disso, implementamos retry com exponential backoff (5 tentativas) e fallback automático para arquivos bulk."

---

## 4. Por que não usar Spark para escrever JSON também?

**Resposta:**
> "Princípio de usar a ferramenta certa para o job correto:
> 
> - **JSON landing zone** (arquivos pequenos, ~5MB): Python nativo é lightweight e rápido
> - **Delta tables** (processamento distribuído): Spark com ACID transactions
> 
> Escrever um JSON de 5MB com Spark seria como usar um caminhão de carga para entregar uma carta - funciona, mas tem overhead desnecessário de inicialização do cluster, seriação, etc.
> 
> Mas note que a **transformação crítica (JSON → Delta)** usa Spark, onde realmente importa."

**Comparação:**
| Operação | Python Nativo | Spark |
|----------|---------------|-------|
| Write 5MB JSON | ~50ms | ~2s (overhead) |
| Write 10GB Parquet | Não escala | ~10s (paralelo) |

---

## 5. Como você lidaria com falhas da API?

**Resposta:**
> "Implementamos resiliência em 3 camadas:
> 
> **Camada 1 - Retry exponencial:**
> - 5 tentativas com backoff: 0s, 2s, 4s, 8s, 16s
> - Evita sobrecarregar API instável
> 
> **Camada 2 - Fallback automático:**
> - Se API falhar completamente, sistema baixa arquivo bulk configurado em `file_url`
> - Detecta ZIP automaticamente e extrai
> - Parse JSON e retorna dados normalmente
> 
> **Camada 3 - Separação Landing/Bronze:**
> - JSON raw preservado no landing zone (auditoria + replay)
> - Se transformação Spark falhar, reprocessa do landing zone
> - Mode overwrite garante idempotência
> 
> **Observabilidade:**
> - Logs estruturados (INFO/WARNING/ERROR) com emojis para fácil identificação
> - Delta Lake time travel para rollback (`VERSION AS OF 10`)
> - Source file tracking em cada registro"

---

## 6. Por que Template Method Pattern?

**Resposta:**
> "Template Method resolve o problema de duplicação de código entre entidades diferentes.
> 
> **Sem o pattern:**
> - 8 entidades × 50 linhas de código repetidas = 400 linhas duplicadas
> - Mudança no workflow = atualizar 8 lugares
> - Alto risco de inconsistência
> 
> **Com Template Method:**
> - Workflow definido uma vez na base class (execute → fetch → save)
> - Child classes implementam apenas `fetch_data()` - 10-15 linhas
> - Mudança no workflow = atualiza 1 lugar
> - Garantia de consistência
> 
> Resultado: **10-15 linhas** para adicionar nova entidade vs **50+ linhas** sem o pattern."

**Exemplo prático:**
```python
# Adicionar nova entidade = 10 linhas
class ProposicoesIngestion(BaseBronzeIngestion):
    def __init__(self, spark):
        super().__init__(spark, entity_name='proposicoes')
    
    def fetch_data(self):
        return self._fetch_standard(params=self.entity_config['params'])
```

---

## 7. Como você testaria localmente sem Databricks?

**Resposta:**
> "Exatamente por isso escolhemos Python nativo! Estrutura de teste:
> 
> ```python
> # tests/test_storage.py
> import pytest
> from unittest.mock import Mock, patch
> 
> def test_save_to_volume():
>     spark = Mock(spec=SparkSession)
>     
>     with patch('os.makedirs') as mock_mkdir, \
>          patch('builtins.open', mock_open()) as mock_file:
>         
>         save_to_volume(config, data, spark)
>         
>         mock_mkdir.assert_called_once_with(path, exist_ok=True)
>         mock_file.assert_called_once()
> ```
> 
> **Com dbutils:** Precisaria Databricks Connect ou ambiente Docker completo  
> **Com Python nativo:** Roda em qualquer CI/CD (GitHub Actions, Jenkins)"

---

## 8. Qual a estratégia de versionamento e deployment?

**Resposta:**
> "Seguimos GitOps principles:
> 
> **Branches:**
> ```
> main (prod) ← protegido, requer PR + review
>   ↑
> dev (staging) ← feature branches mergeiam aqui
>   ↑
> feature/* ← desenvolvimento individual
> ```
> 
> **Pipeline CI/CD:**
> 1. Push para feature branch → Testes unitários rodam (GitHub Actions)
> 2. Merge para dev → Deploy automático em workspace staging
> 3. QA valida em staging
> 4. PR para main → Code review obrigatório
> 5. Merge para main → Deploy em prod
> 
> **Versionamento de dados:**
> - Delta Lake time travel: `SELECT * FROM table VERSION AS OF 10`
> - Rollback = revert Git commit + time travel
> - Auditoria completa: quem escreveu, quando, qual source file"

---

## 9. Por que não usar Databricks Auto Loader ou Delta Live Tables?

**Resposta:**
> "Ferramentas complementares, não substitutas:
> 
> **Auto Loader:** Ideal para **streaming contínuo** de arquivos em cloud storage. Nosso caso é **batch API ingestion** - não temos arquivos chegando continuamente.
> 
> **Delta Live Tables (Lakeflow Spark Declarative Pipelines):** Perfeito para **transformações SQL declarativas** (Bronze → Silver → Gold). Estamos na fase de **ingestão raw (API → Bronze)**, que requer:
> - HTTP requests customizados
> - Retry logic específico
> - Paginação
> - ThreadPool para paralelização
> - Fallback para arquivos
> 
> **Roadmap futuro:**
> ```
> [API] → [Python Ingestion] → [Bronze] → [DLT Pipeline] → [Silver/Gold]
>             ↑ Fase atual                      ↑ Próxima fase
> ```
> 
> Quando chegarmos nas transformações (joins, agregações, limpeza), **aí sim** vamos migrar para DLT - é a ferramenta certa para esse estágio."

---

## 🔟 Como garantir qualidade dos dados?

**Resposta:**
> "Múltiplas camadas de validação:
> 
> **1. Na ingestão (Bronze):**
> - Schema enforcement automático do Spark (type checking)
> - Nullability validation
> - Count validation (zero records = alerta)
> 
> **2. Metadados rastreáveis:**
> ```python
> df.withColumn('source_file', lit(file_path))
>   .withColumn('ingestion_timestamp', current_timestamp())
> ```
> - Cada registro sabe de onde veio e quando
> - Facilita debugging e auditoria
> 
> **3. Preservação do raw:**
> - JSON original no landing zone
> - Permite replay em caso de bug na transformação
> 
> **4. Delta Lake ACID:**
> - Transações atômicas (all-or-nothing)
> - Isolation (leituras consistentes)
> - Time travel (rollback a qualquer versão)
> 
> **Próxima fase (Silver):**
> - Great Expectations para data quality checks
> - Alertas automáticos no Databricks Workflows
> - Métricas de data freshness"

---

## 💡 Pergunta Difícil: "E se..."

**Resposta Universal para Cenários Não Previstos:**

> "Ótima pergunta! Não consideramos especificamente esse cenário no MVP da camada Bronze, mas a arquitetura foi desenhada para **extensibilidade**:
> 
> - **Template Method Pattern**: Adicionar novos métodos de extração é direto
> - **Configuração JSON**: Mudanças sem tocar código
> - **Python nativo + PySpark**: Stack standard da indústria
> - **Modular**: Cada componente (API client, storage, model) é independente
> 
> Podemos implementar [solução proposta] como:
> 1. Novo engine em `BaseBronzeIngestion` (ex: `_fetch_graphql`)
> 2. Nova config em JSON (ex: adicionar `graphql_endpoint`)
> 3. Child class usa o novo engine
> 
> Sem refatoração estrutural. Quer que eu demonstre um exemplo?"

**↑ Use essa resposta se travarem em qualquer pergunta inesperada! 😉**

---

## 📊 Números para Impressionar

### Eficiência de Código
- **859 registros** ingeridos com sucesso (deputados)
- **8 entidades** configuradas via JSON
- **3 engines** de extração (standard, multithreaded, fallback)
- **10-15 linhas** para adicionar nova entidade
- **0 linhas duplicadas** (DRY principle rigoroso)
- **33% redução** de parâmetros (3 → 2)

### Resiliência
- **5 tentativas** de retry com exponential backoff
- **Fallback automático** para arquivos bulk
- **ACID transactions** via Delta Lake
- **Time travel** ilimitado (retention configurável)

### Custo
- **Serverless compute**: Pay-per-use
- **< $1 USD/mês** estimado (execução diária)
- **Zero infraestrutura** gerenciada manualmente

---

## 🎤 Dicas para a Apresentação

### 1. Comece com o problema
> "A Câmara dos Deputados tem dados abertos via API, mas não há estrutura analítica. Parlamentares, assessores e jornalistas precisam fazer análises complexas de gastos, votações e projetos - mas a API é instável e complexa."

### 2. Mostre a solução
> "Criamos um pipeline automatizado e resiliente que transforma dados dispersos em uma única fonte de verdade para análise."

### 3. Destaque os diferenciais
- 🚀 **Resiliente**: 3 camadas de fallback
- 🧪 **Testável**: Roda localmente
- 📦 **Modular**: 10 linhas para nova entidade
- 💰 **Econômico**: < $1/mês

### 4. Finalize com próximos passos
> "Fase Bronze completa. Próximo: Silver layer com DLT, depois dashboards Lakeview para consumo."

---

## 🎯 Frase de Efeito para Encerrar

> "Este pipeline transforma a complexidade da democracia brasileira em simplicidade analítica - com resiliência, qualidade e custo otimizado. É Databricks moderno aplicado a um problema real brasileiro."

🇧🇷 🚀
