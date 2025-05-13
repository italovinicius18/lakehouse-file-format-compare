SELECT
  ano,
  mes,
  sistema_origem,
  COUNT(*) AS total_acompanhamentos
FROM silver_sisvan
GROUP BY ano, mes, sistema_origem;