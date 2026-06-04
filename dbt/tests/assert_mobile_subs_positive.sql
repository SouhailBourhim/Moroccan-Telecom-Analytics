-- Fails if any row has total_subs <= 0 after staging filters
select * from {{ ref('stg_anrt__mobile') }}
where total_subs <= 0
