-- Анализ профессиональных стандартов и зарплат
-- Соответствие профстандартов реальным требованиям рынка труда

SELECT 
    unnest(prof_standards_array) as "Профстандарт",
    technology as "Технология", 
    role as "Роль",
    COUNT(*) as "Вакансий",
    ROUND(AVG(avg_salary)) as "Средняя зарплата"
FROM olap_competency_analysis
WHERE avg_salary IS NOT NULL 
    AND array_length(prof_standards_array, 1) > 0
    AND technology IN ('Python', 'Docker', 'SQL')
    AND role IN ('devops', 'fullstack', 'data')
GROUP BY "Профстандарт", technology, role
HAVING COUNT(*) >= 3
ORDER BY "Средняя зарплата" DESC
LIMIT 15;