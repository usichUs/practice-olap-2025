-- Анализ ФГОС компетенций и зарплат
-- Связь образовательных компетенций с рыночными требованиями и оплатой

SELECT 
    unnest(fgos_competencies_array) as "ФГОС компетенция",
    technology as "Технология",
    role as "Роль",
    COUNT(*) as "Вакансий",
    ROUND(AVG(avg_salary)) as "Средняя зарплата"
FROM olap_competency_analysis
WHERE avg_salary IS NOT NULL 
    AND array_length(fgos_competencies_array, 1) > 0
    AND technology IN ('Python', 'Docker', 'SQL')  -- топ технологии
GROUP BY "ФГОС компетенция", technology, role
HAVING COUNT(*) >= 3
ORDER BY "Средняя зарплата" DESC
LIMIT 15;