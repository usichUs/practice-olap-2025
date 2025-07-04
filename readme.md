# 📊 Анализ компетенций IT-рынка труда

Система комплексного анализа соответствия образовательных стандартов (ФГОС) и профессиональных стандартов требованиям IT-рынка труда на основе данных HeadHunter.

## 🎯 Описание проекта

Проект позволяет:

- 📈 Анализировать спрос на технологии в IT-вакансиях
- 🎓 Сопоставлять требования рынка с компетенциями ФГОС
- 💼 Изучать соответствие профессиональных стандартов реальным вакансиям
- 💰 Анализировать зарплатные предложения по технологиям и ролям
- 📊 Проводить OLAP-анализ многомерных данных

## 🏗️ Архитектура проекта

```
📁 Практика/
├── 📂 csv_files/                     # Финальные данные для анализа
│   ├── fgos_competencies.csv         # ФГОС компетенции
│   ├── otf_td.csv                    # Профстандарты (ОТФ/ТД)
│   ├── hh_vacancies_enhanced_*.csv   # Данные вакансий HH
│   ├── hh_technologies_detailed_*.csv # Технологии с маппингом компетенций
│   └── hh_analytics_*.csv            # Аналитические данные
├── 📂 parsing/                       # Модули сбора данных
│   ├── new_parser.py                 # Основной парсер HH API
│   ├── 📂 FGOS/                      # Работа с ФГОС
│   └── 📂 OTF_TD/                    # Работа с профстандартами
├── 📂 db/                            # Работа с базой данных
│   ├── db_loader.py                  # 🚀 Финальный загрузчик данных
│   ├── check_data.py                 # 🔍 Проверка данных и OLAP готовности
│   ├── mapping.py                    # Маппинг технологий к компетенциям
│   └── create_relationships_fixed.py # Создание связей (опционально)
└── docker-compose.yml                # 🐳 PostgreSQL контейнер
```

## 🔧 Требования

### Системные требования:

- Python 3.8+
- Docker & Docker Compose
- PostgreSQL 14 (через Docker)

### Python пакеты:

```bash
pip install pandas psycopg2-binary requests numpy python-dotenv
```

## 🚀 Быстрый старт

### 1. Подготовка окружения

```bash
# Клонируем/скачиваем проект
cd Практика

# Создаем виртуальное окружение
python3 -m venv venv
source venv/bin/activate  # Linux/Mac
# или venv\Scripts\activate  # Windows

# Устанавливаем зависимости
pip install pandas psycopg2-binary requests numpy python-dotenv
```

### 2. 🐳 Запуск PostgreSQL

```bash
# Поднимаем PostgreSQL 14 контейнер
docker-compose up -d

# Проверяем статус
docker-compose ps
```

**⚠️ Важно:** Используется PostgreSQL 14 для поддержки современных OLAP функций (GROUP BY CUBE, расширенные аналитические функции).

### 3. 📊 Загрузка данных в БД

```bash
cd db/

# Загружаем все данные (ФГОС + профстандарты + HH)
python3 db_loader.py
```

**Что происходит:**

- ✅ Создание оптимизированных таблиц
- 📚 Загрузка ФГОС компетенций
- 💼 Загрузка профессиональных стандартов
- 🏢 Загрузка данных вакансий HH
- 🔧 Загрузка технологий с маппингом
- 📊 Создание OLAP представлений

### 4. 🔍 Проверка данных

```bash
# Проверяем корректность загрузки и готовность к OLAP
python3 check_data.py
```

**Проверяет:**

- 📈 Количество записей в таблицах
- 🔗 Покрытие технологий компетенциями
- 🧊 Работоспособность GROUP BY CUBE
- 📊 Готовность OLAP представлений

## 📊 Структура данных

### 🎯 Основные таблицы:

1. **`vacancy_details`** - Детали вакансий

   - vacancy_id, title, company, role, domain
   - experience_level, avg_salary, area
   - 1,100+ записей с полными данными

2. **`vacancy_technologies_detailed`** - Технологии вакансий

   - technology, category, frequency
   - **fgos_competencies** - связь с ФГОС
   - **prof_standards** - связь с профстандартами
   - 2,500+ записей с маппингом

3. **`fgos_competencies`** - ФГОС компетенции

   - direction_code (02.03.03, 09.03.03, 09.03.04)
   - competency_code, competency_name, competency_type

4. **`otf_td_standards`** - Профессиональные стандарты
   - standard_code (06.001, 06.022)
   - otf_code, td_code, описания ОТФ и ТД

### 📊 OLAP представления:

1. **`olap_competency_analysis`** - Основное для анализа
2. **`tech_market_summary`** - Агрегаты по технологиям
3. **`role_tech_salary_cube`** - Куб роль×технология×зарплата

## 🔍 Примеры анализа

### 1. 🧊 OLAP Куб: Роль × Технология × Зарплата

```sql
SELECT
    role,
    technology,
    salary_range,
    COUNT(*) as vacancy_count,
    AVG(avg_salary) as avg_salary
FROM olap_competency_analysis
WHERE role IS NOT NULL AND technology IS NOT NULL
GROUP BY CUBE(role, technology, salary_range)
ORDER BY vacancy_count DESC;
```

### 2. 🎓 Анализ соответствия ФГОС рынку

```sql
SELECT
    fgos_direction,
    technology,
    COUNT(*) as market_demand,
    AVG(avg_salary) as avg_salary
FROM olap_competency_analysis oca
JOIN fgos_competencies fc ON fc.competency_code = ANY(oca.fgos_competencies_array)
GROUP BY fgos_direction, technology
ORDER BY market_demand DESC;
```

### 3. 💼 Профстандарты vs Реальные требования

```sql
SELECT
    prof_standard,
    technology,
    role,
    COUNT(*) as vacancy_count
FROM olap_competency_analysis oca
JOIN otf_td_standards ots ON ots.td_code = ANY(oca.prof_standards_array)
GROUP BY prof_standard, technology, role
ORDER BY vacancy_count DESC;
```

### 4. 📈 Топ технологий по ролям

```sql
SELECT * FROM tech_market_summary
WHERE vacancy_count > 10
ORDER BY avg_salary DESC;
```

## 📝 Ключевые инсайты проекта

### ✅ **Что уже готово:**

1. **Полная цепочка данных**: ФГОС → Профстандарты → Рынок труда
2. **Маппинг компетенций**: 95%+ технологий связаны с образовательными стандартами
3. **OLAP-готовая структура**: GROUP BY CUBE, многомерный анализ
4. **Качественные данные**: 1,100+ вакансий, 2,500+ записей технологий

### 🎯 **Возможности анализа:**

- **Для университетов**: Корректировка учебных планов под рынок
- **Для студентов**: Понимание карьерных путей и технологий
- **Для работодателей**: Анализ соответствия кандидатов требованиям
- **Для государства**: Оценка эффективности образовательных стандартов

## 🚀 Следующие шаги

### 1. **ClickHouse для OLAP** (рекомендуется)

```bash
# Экспорт данных в ClickHouse для быстрого OLAP
# Создание материализованных представлений
# Настройка кубов и агрегатов
```

### 2. **Визуализация**

- Grafana для дашбордов
- DataLens для интерактивной аналитики
- Jupyter notebooks для исследовательского анализа

### 3. **Расширение данных**

- Добавление других job-сайтов
- Парсинг учебных планов вузов
- Анализ динамики требований во времени

## 🛠️ Конфигурация БД

### PostgreSQL настройки (docker-compose.yml):

```yaml
version: "3.8"
services:
  postgres:
    image: postgres:14
    environment:
      POSTGRES_DB: competency_analysis
      POSTGRES_USER: practice_user
      POSTGRES_PASSWORD: practice_password
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data

volumes:
  postgres_data:
```

### Подключение к БД:

- **Host**: localhost
- **Port**: 5432
- **Database**: competency_analysis
- **User**: practice_user
- **Password**: practice_password

## 🔧 Устранение проблем

### Частые проблемы:

1. **"Ошибка подключения к БД"**

   ```bash
   docker-compose ps  # Проверить статус контейнера
   docker-compose up -d  # Перезапустить если нужно
   ```

2. **"Таблица не найдена"**

   ```bash
   python3 db_loader.py  # Пересоздать таблицы
   ```

3. **"OLAP представления отсутствуют"**
   ```bash
   python3 check_data.py  # Проверить статус
   python3 db_loader.py  # Пересоздать все
   ```

## 📞 Дополнительная информация

- **Время загрузки**: ~2-3 минуты
- **Объем данных**: ~3MB CSV файлов
- **OLAP производительность**: готова для запросов на млн записей
- **Масштабируемость**: легко добавить новые источники данных

---

**🎯 Проект готов к продуктивному OLAP анализу соответствия IT-образования и рынка труда!**
