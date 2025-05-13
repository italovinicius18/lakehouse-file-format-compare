SELECT
  sigla_uf,
  id_municipio,
  estado_nutricional_adulto AS status_nutricional,
  COUNT(*) AS total_status
FROM silver_sisvan
WHERE fase_vida = 'Adulto'
GROUP BY sigla_uf, id_municipio, estado_nutricional_adulto;