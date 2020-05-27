
-- Criar tabela das empresas
CREATE TABLE datasprints.comapy_info(
  vendor_id varchar(100) not null,
  name varchar(100) not null,
  adress varchar(100) not null,
  city varchar(100) not null,
  state varchar(100) not null,
  zip varchar(100) not null,
  country varchar(100) not null,
  contact varchar(100) not null,
  current varchar(100) not null
)

-- Copia dados das empresas do s3 para a tabela
copy datasprints.company_info
from 's3://datasprints-project/data-vendor_lookup-csv.csv'
iam_role 'arn:aws:iam::072265465282:role/rs-fullAccess'
delimiter ';'
ignoreheader 1

-- Cópia do Lookup de pagamento
copy datasprints.payment_info
from 's3://datasprints-project/data-payment_lookup-csv.csv'
iam_role 'arn:aws:iam::072265465282:role/rs-fullAccess'
delimiter ';'
ignoreheader 2

-- Criação da tabela única dos Json
CREATE EXTERNAL SCHEMA travelData FROM DATA CATALOG
DATABASE 'import-taxidata'
REGION 'us-east-1'
IAM_ROLE 'arn:aws:iam::072265465282:role/rs-fullAccess'
CATALOG_ROLE 'arn:aws:iam::072265465282:role/rs-fullAccess'
CREATE EXTERNAL DATABASE IF NOT EXISTS

-- Higinização dos dados de lookup
delete from datasprints.payment_info
where payment_lookup = 'Foo'

-- Criação de uma tabela única
CREATE TABLE datasprints.travel_info AS (
select * from public.data_sample_data_nyctaxi_trips_2009_json_corrigido_json
union
select * from public.data_sample_data_nyctaxi_trips_2010_json_corrigido_json
union
select * from public.data_sample_data_nyctaxi_trips_2011_json_corrigido_json
union
select * from public.data_sample_data_nyctaxi_trips_2012_json_corrigido_json)

-- Separando Campos date e time (Todas ingestão foi feita com string para facilitar o processo)
  -- Cria data do início de viagem
  ALTER TABLE datasprints.travel_info
  ADD COLUMN pickup_date varchar(20);

  update datasprints.travel_info
  SET pickup_date = substring(pickup_datetime, 1,10);

  -- Cria hora do início de viagem
  ALTER TABLE datasprints.travel_info
  ADD COLUMN pickup_date varchar(20);

  update datasprints.travel_info
  SET pickup_date = substring(pickup_datetime, 1,10);

  -- Cria data de fim da viagem
  ALTER TABLE datasprints.travel_info
  ADD COLUMN dropoff_date varchar(20);

  update datasprints.travel_info
  SET dropoff_date = substring(dropoff_datetime, 1,10);

  -- Cria hora de fuim da viagem
  ALTER TABLE datasprints.travel_info
  ADD COLUMN dropoff_time varchar(20);

  update datasprints.travel_info
  SET dropoff_time = substring(dropoff_datetime, 12,8);

-- Média da distance de viagens com até 2 passageiros
select
	avg(trip_distance) as mean_distance
FROM datasprints.travel_info
where passenger_count in (1,2);

-- Corrige o payment_type
create table datasprints.travel_data as
select
	vendor_id,
	to_date(dropoff_date, 'YYYY-MM-DD') as dropoff_date,
  dropoff_time,
	dropoff_datetime,
	dropoff_latitude,
	dropoff_longitude,
	fare_amount,
	passenger_count,
  to_date(pickup_date, 'YYYY-MM-DD') as pickup_date,
  pickup_time,
	pickup_datetime,
	pickup_latitude,
	pickup_longitude,
	rate_code,
	store_and_fwd_flag,
	cast(surcharge as numeric(10,5)),
	cast(tip_amount as numeric(10,5)),
	cast(tolls_amount as numeric(10,5)),
	cast(total_amount as numeric(10,5)),
	cast(trip_distance as numeric(10,5)),
  payment_info.payment_lookup
FROM datasprints.travel_info join datasprints.payment_info on payment_info.payment_type = travel_info.payment_type

-- Criação de coluna com o Valor que vai para empresa
alter table datasprints.travel_data
add column vendor_revenue numeric(10,5);

UPDATE datasprints.travel_data
SET vendor_revenue = (CASE
    	WHEN tip_amount is null THEN total_amount
    	WHEN tip_amount is not null THEN (total_amount - tip_amount)
    END)

-- Adiciona coluna year_month na tabela fato travel_data
alter table datasprints.travel_data
add column year_month varchar(30);
update datasprints.travel_data
set year_month = ltrim(to_char(pickup_date::date,'YYYY-MM'), '0');

-- Adiciona coluna year na tabela fato travel_data
alter table datasprints.travel_data
add column year varchar(4);
update datasprints.travel_data
set year = substring(year_month, 1, 4);

-- Cria tabela Vendors_ranking
create table Vendors_ranking as
SELECT
  sum(vendor_revenue) as revenue,
  vendor_id
from datasprints.travel_data
where payment_lookup = 'Cash'
group by vendor_id
order by revenue desc;

-- Cria tabela amount_travel_month
create table datasprints.amount_travel_month as
select
	count(passenger_count) as qtd_corridas,
    year_month,
    year
from datasprints.travel_data
where payment_lookup = 'Cash'
group by year_month
order by year_month desc;

-- Cria tabela de tempo de viagem
create table datasprints.wkd_data as
select
	cast(concat(concat(to_char(pickup_date, 'YYYY-MM-DD'),' '), pickup_time) as timestamp) as tempo_ini,
	cast(concat(concat(to_char(dropoff_date, 'YYYY-MM-DD'),' '), dropoff_time) as timestamp) as tempo_fim,                                              	datediff(minutes, tempo_ini, tempo_fim) as tempo,
	date_part(dow, pickup_date) as dia_semana_pick,
	date_part(dow, dropoff_date) as dia_semana_drop
from datasprints.travel_data

-- média tempo de viagens no fim de semana
select
	(avg(tempo)) as tempo_medio
from datasprints.wkd_data