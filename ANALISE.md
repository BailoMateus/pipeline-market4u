# Análise Final - Top Clientes em São Paulo

## Pergunta de Negócio
"Quais são os 10 clientes que mais gastaram (maior total_gasto) no estado de São Paulo (SP) e qual a categoria de produto preferida de cada um deles?"

---

## Query Utilizada
```plaintext
SELECT
    customer_unique_id,
    total_gasto,
    categoria_mais_comprada
FROM gold.dm_vendas_clientes
WHERE estado_cliente = 'SP'
ORDER BY total_gasto DESC
LIMIT 10;
```
---

## Explicação
- A query consulta diretamente o Data Mart dm_vendas_clientes criado na camada Gold.
- Filtramos apenas clientes de São Paulo (SP).
- Ordenamos pelo total_gasto, que já considera price + freight_value.
- Limitamos a 10 resultados, retornando os top 10 clientes por gasto total.
- Incluímos a categoria de produto mais comprada, já calculada no processo Gold.

---

## Resultados Esperados
- Identificação dos clientes mais valiosos (maior gasto).
- Entendimento de sua categoria de consumo preferida.
- Essa análise pode ser usada para ações de marketing direcionadas e estratégias de retenção de clientes de alto valor.
