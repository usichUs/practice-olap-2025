-- OLAP куб: роль × технология × зарплата
-- Многомерный анализ пересечения ролей и технологий для высокооплачиваемых позиций

SELECT 
    role as "Роль",
    technology as "Технология",
    COUNT(*) as "Вакансий",
    ROUND(AVG(avg_salary)) as "Средняя зарплата"
FROM olap_competency_analysis
WHERE avg_salary IS NOT NULL 
    AND role IN ('mobile', 'devops', 'fullstack', 'data')  -- топ-4 по зарплатам
GROUP BY role, technology
HAVING COUNT(*) >= 5  -- минимум 5 вакансий
ORDER BY "Средняя зарплата" DESC;