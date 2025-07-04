-- Анализ высокооплачиваемых комбинаций
-- Итоговый отчет наиболее прибыльных сочетаний ролей и технологий

SELECT 
    role as "Роль",
    technology as "Технология", 
    COUNT(*) as "Вакансий",
    ROUND(AVG(avg_salary)) as "Средняя зарплата",
    ROUND(MIN(avg_salary)) as "Минимум",
    ROUND(MAX(avg_salary)) as "Максимум"
FROM olap_competency_analysis
WHERE avg_salary IS NOT NULL 
    AND avg_salary >= 200000  -- только высокооплачиваемые вакансии
GROUP BY role, technology
HAVING COUNT(*) >= 3  -- минимум 3 вакансии
ORDER BY "Средняя зарплата" DESC;