with
    stg_clientes as (
        select *
        from {{ ref('stg_erp_clientes') }}
    )

    , criar_chave as (
        select
            row_number() over(order by id_cliente) as pk_cliente
            , id_cliente
            , nome_cliente
            , empresa_cliente
            , endereco_cliente
            , cep_cliente
            , cidade_cliente
            , regiao_cliente
            , pais_cliente
        from stg_clientes
    )

select *
from criar_chave