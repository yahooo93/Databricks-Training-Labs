-- Databricks notebook source
use f1_presentation;
show tables

-- COMMAND ----------

create table f1_presentation.calculated_race_results
using parquet
as
select ra.race_year,
c.name as team_name,
d.name as driver_name,
r.position,
r.points,
11- r.position as calculated_points
from f1_processed.results r
join f1_processed.drivers d on r.driver_id = d.driver_id
join f1_processed.constructors c on r.constructor_id = c.constructor_id
join f1_processed.races ra on r.race_id = ra.race_id 
where r.position <= 10

-- COMMAND ----------


