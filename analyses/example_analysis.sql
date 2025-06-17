-- Analyse pour comprendre la distribution des données
SELECT
    DATE_TRUNC('month', created_at) as month,
    COUNT(*) as count
FROM {{ ref('example_model') }}
GROUP BY 1
ORDER BY 1 