SELECT
  ano,
  mes,
  sigla_uf,
  COUNT(*) AS total_acompanhamentos
FROM silver_sisvan
GROUP BY ano, mes, sigla_uf;