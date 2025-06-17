# Documentação CNPJ Data

Documentação técnica completa sobre o formato e estrutura dos dados CNPJ (Cadastro Nacional da Pessoa Jurídica) do Brasil.

## Navegação Rápida

- [Visão Geral](data-schema/overview.md) - Entendendo os dados CNPJ
- [Schemas CSV](data-schema/csv-schemas.md) - Especificações campo a campo
- [Mapeamentos](data-schema/field-mappings.md) - Conversões e transformações
- [Relacionamentos](data-schema/relationships.md) - Estrutura relacional
- [Qualidade de Dados](guides/data-quality.md) - Problemas conhecidos e soluções
- [Fonte de Dados](guides/data-source.md) - Como obter os arquivos
- [Exemplos](sample-data/) - Arquivos para testes

## Origem dos Dados

Os dados CNPJ são publicados mensalmente pela Receita Federal do Brasil em:
https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj

## Conceitos Fundamentais

### CNPJ
Número de 14 dígitos que identifica pessoa jurídica no Brasil:
- **CNPJ Básico**: Primeiros 8 dígitos (identifica a empresa)
- **Ordem**: Próximos 4 dígitos (identifica o estabelecimento)
- **DV**: Últimos 2 dígitos (verificação)

### CNAE
Classificação Nacional de Atividades Econômicas - código de 7 dígitos que identifica a atividade principal da empresa.

### Estabelecimento
Localização física da empresa. Uma empresa (CNPJ básico) pode ter múltiplos estabelecimentos.

## Estrutura dos Dados

### Volume
- **50+ milhões** de empresas
- **60+ milhões** de estabelecimentos
- **20+ milhões** de sócios
- **~15GB** comprimido / **~85GB** descomprimido

### Formato
- **Encoding**: ISO-8859-1 (Latin-1)
- **Separador**: Ponto e vírgula (;)
- **Datas**: Formato YYYYMMDD (00000000 = nulo)
- **Decimais**: Vírgula como separador

## Notas Técnicas

### Mascaramento de CPF
Por questões de privacidade, CPFs são parcialmente ocultados seguindo art. 129 § 2º da Lei 13.473/2017:
- Formato: `***XXXXXX**`
- Oculta 3 primeiros e 2 últimos dígitos

### Atualizações
- Frequência: Mensal (primeira semana)
- Defasagem: Dados com 30-45 dias de atraso
- Histórico: Mantém snapshot completo

---

# CNPJ Data Documentation (English)

Complete technical documentation for Brazilian CNPJ (company registry) data format and structure.

## Quick Links

- [Overview](data-schema/overview.md) - Understanding CNPJ data
- [CSV Schemas](data-schema/csv-schemas.md) - Field specifications
- [Field Mappings](data-schema/field-mappings.md) - Transformations
- [Relationships](data-schema/relationships.md) - Data structure
- [Data Quality](guides/data-quality.md) - Known issues
- [Data Source](guides/data-source.md) - How to obtain files
- [Samples](sample-data/) - Test files

## Data Source

Monthly publication by Brazilian Federal Revenue at:
https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj

## Key Concepts

- **CNPJ**: 14-digit company ID (8 base + 4 branch + 2 check)
- **CNAE**: 7-digit economic activity code
- **Establishment**: Physical location of company

## Technical Notes

- Format: ISO-8859-1, semicolon-separated
- Size: ~15GB compressed / ~85GB uncompressed
- Updates: Monthly with 30-45 day delay
- Privacy: CPF numbers partially masked by law
