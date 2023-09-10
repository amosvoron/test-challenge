SELECT MIN(A.name) AS name
	, MIN(A.email) AS email
	, SUM(C.price * B.quantity) AS total_amount
FROM users AS A
INNER JOIN orders AS B ON A.id = B.user_id
INNER JOIN products AS C ON B.product_id = C.id
WHERE C.category = 'Electronics'
GROUP BY A.id
HAVING COUNT(*) >= 3 AND SUM(C.price * B.quantity) > 1000
ORDER BY total_amount DESC;
