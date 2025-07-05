# 🇧🇷 CNPJ Data Pipeline

![CNPJ Data Pipeline](https://img.shields.io/badge/version-1.0.0-blue.svg) ![License](https://img.shields.io/badge/license-MIT-green.svg) ![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)

Um script modular e configurável para processar arquivos CNPJ da Receita Federal do Brasil. Este projeto é ideal para quem precisa trabalhar com dados de empresas, oferecendo um processamento inteligente de mais de 50 milhões de registros, com suporte a múltiplos bancos de dados.

## Características Principais

- **Arquitetura Modular**: A estrutura do projeto separa claramente as responsabilidades, permitindo fácil manutenção e escalabilidade. Cada módulo tem uma função específica, facilitando a adição de novas funcionalidades no futuro.

- **Multi-Banco**: O PostgreSQL é totalmente suportado. Além disso, há placeholders para integração com MySQL, BigQuery e SQLite, permitindo que você escolha o banco de dados que melhor se adapta às suas necessidades.

- **Processamento Inteligente**: O sistema adapta automaticamente sua estratégia de processamento com base nos recursos disponíveis. Isso garante eficiência mesmo em ambientes com limitações de hardware.

- **Downloads Paralelos**: O projeto oferece uma estratégia configurável para otimizar a velocidade de download. Você pode ajustar o número de downloads simultâneos para maximizar a eficiência.

- **Processamento Incremental**: O sistema rastreia arquivos já processados para evitar duplicações. Isso é crucial quando você lida com grandes volumes de dados.

- **Performance Otimizada**: As operações em bulk são eficientes, e o tratamento de conflitos é bem gerenciado, garantindo que você tenha dados consistentes e atualizados.

- **Configuração Simples**: O setup é interativo e fácil de seguir. Você também pode usar variáveis de ambiente para personalizar sua configuração.

## Início Rápido

### Opção 1: Setup Interativo (Recomendado)

Para começar, você pode clonar o repositório usando o seguinte comando:

```bash
# Clone o repositório
git clone https://github.com/nxoti/cnpj-data-pipeline
```

Após clonar o repositório, entre na pasta do projeto e siga as instruções do arquivo `README.md` para configurar seu ambiente.

### Opção 2: Download Manual

Se preferir, você pode baixar o arquivo diretamente da seção de [Releases](https://github.com/nxoti/cnpj-data-pipeline/releases). Basta escolher a versão desejada e seguir as instruções para execução.

## Estrutura do Projeto

O projeto é organizado da seguinte forma:

```
cnpj-data-pipeline/
├── src/
│   ├── main.py
│   ├── database/
│   ├── processing/
│   └── utils/
├── config/
│   ├── config.yaml
│   └── .env
├── tests/
│   └── test_main.py
└── README.md
```

- **src/**: Contém o código-fonte do projeto.
- **config/**: Armazena arquivos de configuração.
- **tests/**: Inclui testes automatizados para garantir a qualidade do código.

## Requisitos

Antes de começar, verifique se você tem os seguintes requisitos instalados:

- Python 3.6 ou superior
- PostgreSQL ou outro banco de dados compatível
- pip (gerenciador de pacotes do Python)

Você pode instalar as dependências necessárias com o seguinte comando:

```bash
pip install -r requirements.txt
```

## Configuração do Banco de Dados

Para configurar o banco de dados, você deve editar o arquivo `config/config.yaml`. Este arquivo contém todas as configurações necessárias para conectar ao seu banco de dados.

Aqui está um exemplo de como o arquivo pode ser estruturado:

```yaml
database:
  type: postgresql
  host: localhost
  port: 5432
  user: seu_usuario
  password: sua_senha
  database: nome_do_banco
```

Certifique-se de substituir os valores de exemplo pelos seus dados reais.

## Executando o Projeto

Depois de configurar o banco de dados, você pode executar o script principal. Navegue até a pasta `src/` e execute o seguinte comando:

```bash
python main.py
```

Isso iniciará o processo de download e processamento dos arquivos CNPJ.

## Testes

O projeto inclui uma suíte de testes para garantir que tudo funcione corretamente. Você pode executar os testes com o seguinte comando:

```bash
pytest tests/
```

Isso irá rodar todos os testes definidos na pasta `tests/`.

## Contribuindo

Contribuições são bem-vindas! Se você deseja contribuir com o projeto, siga estas etapas:

1. Fork o repositório.
2. Crie uma nova branch (`git checkout -b feature/nova-funcionalidade`).
3. Faça suas alterações e commit (`git commit -m 'Adiciona nova funcionalidade'`).
4. Envie para o repositório remoto (`git push origin feature/nova-funcionalidade`).
5. Abra um Pull Request.

## Licença

Este projeto está licenciado sob a Licença MIT. Consulte o arquivo `LICENSE` para mais detalhes.

## Links Úteis

Para mais informações, visite a seção de [Releases](https://github.com/nxoti/cnpj-data-pipeline/releases) para baixar as versões mais recentes do projeto. Aqui você encontrará atualizações e melhorias contínuas.

Sinta-se à vontade para explorar o código, fazer perguntas ou contribuir com melhorias. Estamos sempre abertos a sugestões e colaborações.