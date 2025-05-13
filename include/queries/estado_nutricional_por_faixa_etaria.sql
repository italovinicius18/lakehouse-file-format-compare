SELECT
  fase_vida,
  sexo,
  estado_nutricional_adulto AS estado_nutricional,
  COUNT(*) AS total
FROM silver_sisvan
WHERE estado_nutricional_adulto IS NOT NULL
GROUP BY fase_vida, sexo, estado_nutricional_adulto;