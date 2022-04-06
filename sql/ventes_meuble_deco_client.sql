SELECT client_id, sum(meuble) AS ventes_meuble, sum(deco) AS ventes_deco
FROM (
	SELECT transac.client_id, 
		CASE WHEN pn.product_type='MEUBLE' THEN transac.prod_price*transac.prod_qty ELSE 0 AS meuble,
		CASE WHEN pn.product_type='DECO' THEN transac.prod_price*transac.prod_qty ELSE 0 AS deco
	FROM TRANSACTIONS AS transac
	LEFT JOIN PRODUCT_NOMENCLATURE AS pn
	ON pn.product_id = transac.prod_id
	WHERE transac.date BETWEEN '2020-01-01' AND '2020-12-31'
)
GROUP BY client_id