-- Анализ технологий и зарплат
-- Определение наиболее высокооплачиваемых технологий с анализом спроса

SELECT 
    technology as "Технология",
    COUNT(*) as "Количество вакансий",
    ROUND(AVG(avg_salary)) as "Средняя зарплата",
    COUNT(DISTINCT role) as "Количество ролей"
FROM olap_competency_analysis
WHERE avg_salary IS NOT NULL
GROUP BY technology
HAVING COUNT(*) >= 10  -- только технологии с 10+ вакансиями
ORDER BY "Средняя зарплата" DESC;