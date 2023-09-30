with
    fonte_transportadores as (
            select
           cast(shipper_id as int) as id_transportadora 
           , cast(company_name as string) as nome_transportador
           -- cast(phone as int) as nome_telefone
            from {{ source( 'erp', 'shippers') }}
    )

select *
from fonte_transportadores