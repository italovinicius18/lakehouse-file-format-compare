SELECT
  ano,
  COUNT(DISTINCT id_individuo) AS total_unicos
FROM silver_sisvan
GROUP BY ano;