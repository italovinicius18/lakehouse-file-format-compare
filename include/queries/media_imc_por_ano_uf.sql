SELECT
  ano,
  sigla_uf,
  ROUND(AVG(imc), 2) AS media_imc
FROM silver_sisvan
WHERE imc IS NOT NULL
  AND imc > 0
GROUP BY ano, sigla_uf;