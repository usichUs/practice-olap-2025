-- Анализ пробелов рынка и образования (ИСПРАВЛЕНО)
-- Выявление несоответствий между образовательными программами и рыночными требованиями

WITH market_demand AS (
    SELECT 
        technology,
        COUNT(*) as market_demand,
        AVG(avg_salary) as avg_salary
    FROM olap_competency_analysis
    GROUP BY technology
),
fgos_coverage AS (
    SELECT 
        technology,
        array_length(fgos_competencies_array, 1) as fgos_competencies_count
    FROM olap_competency_analysis
    WHERE array_length(fgos_competencies_array, 1) > 0
    GROUP BY technology, fgos_competencies_array
)
SELECT 
    md.technology as "Технология",
    md.market_demand as "Рыночный спрос",
    ROUND(md.avg_salary) as "Средняя зарплата",
    COALESCE(MAX(fc.fgos_competencies_count), 0) as "Покрытие ФГОС",
    CASE 
        WHEN MAX(fc.fgos_competencies_count) IS NULL THEN 'Нет покрытия ФГОС'
        WHEN MAX(fc.fgos_competencies_count) < 2 THEN 'Низкое покрытие'
        ELSE 'Хорошее покрытие'
    END as "Уровень покрытия"
FROM market_demand md
LEFT JOIN fgos_coverage fc ON md.technology = fc.technology
GROUP BY md.technology, md.market_demand, md.avg_salary
ORDER BY md.market_demand DESC;