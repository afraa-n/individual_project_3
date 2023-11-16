SELECT
    species,
    AVG(petal_length * petal_width) AS avg_petal_area
FROM
    default.iris
GROUP BY
    species;
