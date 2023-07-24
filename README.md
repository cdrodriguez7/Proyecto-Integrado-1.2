# Proyecto-Integrado-1.2

Para la carga de datos debe estar instalado pandas y mysql-connector
```python
pip install pandas
pip install mysql-connector
```
Pasos para carga de datos
SELECT c.nombre_canton, p.tasa_desempleo, v.tipo_vivienda, AVG(h.valor_arriendo) AS valor_arriendo_promedio, 
MAX(da.luz_incluida) AS luz_incluida, pa.nombre_parroquia
FROM provincia p
INNER JOIN canton c ON p.cod_provincia = c.provincia_canton
INNER JOIN parroquia pa ON c.cod_canton = pa.cod_canton
INNER JOIN vivienda v ON pa.cod_parroquia = v.cod_parroquia
INNER JOIN hogar h ON v.id_vivienda = h.id_vivienda
LEFT JOIN datos_arriendo da ON h.id_hogar = da.id_hogar
WHERE c.nombre_canton = 'Loja' 
GROUP BY c.nombre_canton, p.tasa_desempleo, v.tipo_vivienda, pa.nombre_parroquia;
