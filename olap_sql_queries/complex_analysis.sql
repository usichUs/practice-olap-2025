-- Финальный комплексный отчет
-- Связь образовательных стандартов с высокооплачиваемыми позициями

SELECT 
    role as "Роль",
    technology as "Технология",
    COUNT(*) as "Вакансий",
    ROUND(AVG(avg_salary)) as "Средняя зарплата",
    MIN(avg_salary) as "Минимум",
    MAX(avg_salary) as "Максимум",
    (fgos_competencies_array[1]) as "ФГОС компетенция",
    (prof_standards_array[1]) as "Профстандарт"
FROM olap_competency_analysis
WHERE avg_salary IS NOT NULL 
    AND array_length(fgos_competencies_array, 1) > 0
    AND array_length(prof_standards_array, 1) > 0
    AND avg_salary >= 200000  -- только высокооплачиваемые
GROUP BY role, technology, fgos_competencies_array[1], prof_standards_array[1]
ORDER BY "Средняя зарплата" DESC;