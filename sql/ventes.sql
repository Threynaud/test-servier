SELECT date, SUM(prod_price*prod_qty) as ventes
FROM TRANSACTIONS
WHERE date between '2019-01-01' and '2019-12-31'
GROUP BY date
ORDER BY date ASC