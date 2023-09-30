with
    stg_transportadores as (
        select *
        from {{ ref( 'stg_erp_transportadores') }}
    )

, criar_chave as (
    select 
        row_number () over(order by id_transportadora) as pk_transportadora
        , id_transportadora
        , nome_transportador
    from stg_transportadores
    )

select *
from criar_chave