-- Анализ зарплат по IT-ролям
-- Базовый анализ распределения зарплат по профессиональным ролям

SELECT 
    role as "Роль",
    COUNT(*) as "Количество вакансий",
    ROUND(AVG(avg_salary)) as "Средняя зарплата"
FROM olap_competency_analysis
WHERE avg_salary IS NOT NULL
GROUP BY role
ORDER BY "Средняя зарплата" DESC;