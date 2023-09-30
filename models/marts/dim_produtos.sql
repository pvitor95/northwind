with
    stg_categories as (
        select *
        from {{ ref ('stg_erp__categories') }}
    )

    , stg_fornecedores as (
        select *
        from {{ ref('stg_erp_fornecedores')}}
    )

    , stg_produtos as (
        select *
        from {{ ref('stg_erp_produtos') }}  
    )

    , join_tabelas as (
        select
        stg_produtos.id_produto
        , stg_produtos.nome_produto
        , stg_produtos.id_fornecedor
        , stg_produtos.id_categoria
        , stg_produtos.quantidade_por_unidade
        , stg_produtos.preco_por_unidade
        , stg_produtos.unidades_em_estoque
        , stg_produtos.unidades_por_ordem
        , stg_produtos.nivel_reabastecimento
        , stg_produtos.is_discontinuado
        , stg_categories.nome_categoria
        , stg_categories.descricao_categoria
        , stg_fornecedores.nome_fornecedor
        , stg_fornecedores.contato_fornecedor
        , stg_fornecedores.contato_funcao
        , stg_fornecedores.endereco_fornecedor
        , stg_fornecedores.cidade_fornecedor
        , stg_fornecedores.regiao_fornecedor
        , stg_fornecedores.cep_fornecedor
        , stg_fornecedores.pais_fornecedor

        from stg_produtos
        left join stg_categories on 
            stg_produtos.id_categoria = stg_categories.id_categoria
        left join stg_fornecedores on
            stg_produtos.id_fornecedor = stg_fornecedores.id_fornecedor

    )

    , criar_chave as (
        select
            row_number() over(order by id_produto) as pk_produto
            , *
        from join_tabelas
    )

select *
from criar_chave