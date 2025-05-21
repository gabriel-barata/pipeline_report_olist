# Olist Report Pipeline

## Usage Example
```bash
poetry env activate
```

```bash
poetry run python -m jobs.main landing.kaggle_to_s3 '2025-05-20'
```


## Datalake Layers
### Landing
Essa camada do Lake é destinada a receber os dados da origem, exatamente como foram extráidos. Neste layer não temos restrição quanto ao formato de arquivo, podendo ser CSV, JSON, PARQUET, etc. 
Também temos uma regra definida para a retenção dos dados nessa camada, após 7 dias no bucket, eles são escluídos.

### Raw
Camada destinada a receber o exato mesmo data presente na camada **Landing**, porém normalizado e convertido para um formato otimizado para leitura. Nesta camada são aceitos apenas dados no formato `parquet` e não há regra quanto a retenção dos dados.

### Curated
Camada destinada aos dados enriquecidos, com joins e transformações. Nesta camada todas as tabelas devem ser escritas em formato `delta`. E, se possível, catalogados (no caso de haver um catalogo de dados presente).
