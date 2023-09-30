with
    orders as (
        select *
        from {{ ref( 'stg_erp_orders')}}
    )

, orders_details as (
        select *
        from {{ ref( 'stg_erp_orders_details')}}
)

, join_tabelas as (
    select
    orders.id_pedido 
            , orders.id_funcionario
            , orders.id_cliente
            , orders.id_transportadora
            , orders_details.id_produto
            , orders.data_do_pedido
            , orders.frete
            , orders.destinatario
            , orders.endereco_destinatario
            , orders.cep_destinatario
            , orders.cidade_destinatario
            , orders.regiao_destinatario
            , orders.pais_destinatario
            , orders.data_do_envio
            , orders.data_requerida_entrega
            , orders_details.desconto_perc
            , orders_details.preco_da_unidade
            , orders_details.quantidade

    from orders_details
    left join orders on
        orders_details.id_pedido = orders.id_pedido
)

, criar_chave as (
    select
        cast(id_pedido as string) || cast(id_produto as string) as sk_pedido_item
        , *
    from join_tabelas
)

select *
from criar_chave