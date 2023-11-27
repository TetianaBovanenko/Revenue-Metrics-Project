WITH payments AS (
	SELECT
		payment_date,
		user_id,
		revenue_amount_usd,
		substring(date_trunc('month', payment_date) :: TEXT, 1, 10) :: date AS payment_month
	FROM
		games_payments
),
user_payments AS (
SELECT
	payment_month,
	user_id,
	sum(revenue_amount_usd) AS revenue,
	FIRST_VALUE (payment_month) OVER (
		PARTITION BY user_id
		ORDER BY payment_month
	) AS payment_first_month,
		CASE 
		WHEN payment_month = FIRST_VALUE (payment_month) OVER (
			PARTITION BY user_id
			ORDER BY payment_month
		) THEN 1
		ELSE 0
	END AS payment_first,
		CASE 
		WHEN payment_month = FIRST_VALUE (payment_month) OVER (
			PARTITION BY user_id
			ORDER BY payment_month
		) THEN sum(revenue_amount_usd)
		ELSE 0
	END AS revenue_first,
	substring((payment_month + INTERVAL '1 month') :: TEXT, 1, 10) :: date AS churned_month,
	CASE 
		WHEN substring((payment_month + INTERVAL '1 month') :: TEXT, 1, 10) :: date = LEAD (payment_month) OVER (
			PARTITION BY user_id
			ORDER BY payment_month
		) THEN 0
		ELSE 1
	END AS churned_user,
	CASE 
		WHEN substring((payment_month + INTERVAL '1 month') :: TEXT, 1, 10) :: date = LEAD (payment_month) OVER (
			PARTITION BY user_id
			ORDER BY payment_month
		) THEN 0
		ELSE sum(revenue_amount_usd)
	END AS churned_revenue,
	CASE
		WHEN payment_month !=  substring((LAG (payment_month) OVER (
			PARTITION BY user_id
			ORDER BY payment_month
		) + INTERVAL '1 month') :: TEXT, 1, 10) :: date THEN 0
		WHEN sum(revenue_amount_usd) > LAG (sum(revenue_amount_usd)) OVER (
			PARTITION BY user_id
			ORDER BY payment_month
		) THEN sum(revenue_amount_usd) - COALESCE (LAG (sum(revenue_amount_usd)) OVER (
			PARTITION BY user_id
			ORDER BY payment_month
		), 0)
		ELSE 0 
	END AS expansion_revenue,
	CASE
		WHEN payment_month !=  substring((LAG (payment_month) OVER (
			PARTITION BY user_id
			ORDER BY payment_month
		) + INTERVAL '1 month') :: TEXT, 1, 10) :: date THEN 0
		WHEN sum(revenue_amount_usd) < LAG (sum(revenue_amount_usd)) OVER (
			PARTITION BY user_id
			ORDER BY payment_month
		) THEN sum(revenue_amount_usd) - LAG (sum(revenue_amount_usd)) OVER (
			PARTITION BY user_id
			ORDER BY payment_month
		)
		ELSE 0 
	END AS contraction_revenue,
	date_part('month', age(FIRST_VALUE (payment_month) OVER (
		PARTITION BY user_id
		ORDER BY payment_month DESC 
	), FIRST_VALUE (payment_month) OVER (
		PARTITION BY user_id
	))) AS lt
FROM
	payments AS gp
GROUP BY
	payment_month, user_id 
ORDER BY 
	1, 2
)
SELECT
	payment_month,
	up.user_id,
	gpu.language,
	gpu.age,
	payment_first_month,
	payment_first,
	revenue_first,
	churned_month,
	churned_user,
	churned_revenue,
	expansion_revenue,
	contraction_revenue,
	lt
FROM 
	user_payments AS up
LEFT JOIN 
	games_paid_users AS gpu ON up.user_id = gpu.user_id 