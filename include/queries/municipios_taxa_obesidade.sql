SELECT
  sigla_uf,
  id_municipio,
  ROUND(
    SUM(CASE WHEN estado_nutricional_adulto LIKE 'Obesidade%' THEN 1 ELSE 0 END) * 100.0
    / COUNT(*),
    2
  ) AS taxa_obesidade
FROM silver_sisvan
WHERE fase_vida = 'Adulto'
GROUP BY sigla_uf, id_municipio
ORDER BY taxa_obesidade DESC;