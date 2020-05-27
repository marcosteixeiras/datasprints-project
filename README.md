## Análises sobre os dados das comapnhias de taxi de NY ##

O processo de análise foi construído seguindo os passos abaixo:

## Preparação do Ambiente ##

1. Conta AWS
2. Criação de Bucket S3
3. Criação do cluster Redshift
4. Instalação do Jupyter Notebook
5. Instalação das bibliotecas Python (Pandas, Numpy, psycopg2)

## Processo analítico ##

1. Upload dos arquivos (Json e CSV) para o AWS S3
2. Para os arquivos Json foi criados crawlers e jobs no AWS Glue para fazer de forma automatizada a ingestão dos dados no Redshift
3. Para os arquivos CSV foi utilizado comando COPY no editor de querys do Redshift
4. Após os dados no RedShift, executar as querys contidas no arquivo querys.sql
5. Em paralelo foi realizada a construção dos gráficos utilizando Python usando o Jupyter Notebook (feito fora da AWS)


