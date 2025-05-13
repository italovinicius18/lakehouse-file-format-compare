SELECT
  sistema_origem,
  COUNT(*) AS total
FROM silver_sisvan
GROUP BY sistema_origem;