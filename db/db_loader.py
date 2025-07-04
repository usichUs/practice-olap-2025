import psycopg2
import pandas as pd
import numpy as np
import os
import sys
from datetime import datetime

# Конфигурация подключения
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'competency_analysis',
    'user': 'practice_user',
    'password': 'practice_password'
}

def get_connection():
    """Создание подключения к БД"""
    try:
        return psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        print(f"❌ Ошибка подключения к БД: {e}")
        sys.exit(1)

def clean_data(value, data_type='string', max_length=None):
    """Универсальная очистка данных"""
    if pd.isna(value) or value == '' or value is None:
        return None
    
    if data_type == 'string':
        result = str(value).strip()
        if max_length:
            result = result[:max_length]
        return result if result else None
    
    elif data_type == 'integer':
        try:
            num_value = float(value)
            if pd.isna(num_value) or np.isinf(num_value):
                return None
            return int(num_value) if num_value <= 2147483647 else 2147483647
        except (ValueError, TypeError, OverflowError):
            return None
    
    elif data_type == 'bigint':
        try:
            num_value = float(value)
            if pd.isna(num_value) or np.isinf(num_value):
                return None
            # Ограничиваем разумными пределами для зарплат
            if num_value > 10000000:
                return 10000000
            if num_value < 0:
                return None
            return int(num_value)
        except (ValueError, TypeError, OverflowError):
            return None
    
    return value

def create_final_tables():
    """Создание финальных таблиц для OLAP анализа"""
    conn = get_connection()
    cur = conn.cursor()
    
    print("🔧 Создание финальных таблиц для OLAP...")
    
    try:
        # Очищаем существующие таблицы
        tables_to_drop = [
            'vacancy_technologies_detailed',
            'vacancy_details', 
            'fgos_competencies',
            'otf_td_standards'
        ]
        
        for table in tables_to_drop:
            cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
        
        # 1. Таблица ФГОС компетенций
        cur.execute("""
            CREATE TABLE fgos_competencies (
                id SERIAL PRIMARY KEY,
                direction_code VARCHAR(20) NOT NULL,
                direction_name VARCHAR(500) NOT NULL,
                competency_code VARCHAR(20) NOT NULL,
                competency_name TEXT NOT NULL,
                competency_description TEXT,
                competency_type VARCHAR(20),
                category VARCHAR(100),
                level_description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                UNIQUE(direction_code, competency_code)
            )
        """)
        
        # 2. Таблица профессиональных стандартов (OTF/TD)
        cur.execute("""
            CREATE TABLE otf_td_standards (
                id SERIAL PRIMARY KEY,
                standard_code VARCHAR(20) NOT NULL,
                otf_code VARCHAR(10) NOT NULL,
                otf_name VARCHAR(500) NOT NULL,
                td_code VARCHAR(20) NOT NULL,
                td_name TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                UNIQUE(standard_code, td_code)
            )
        """)
        
        # 3. Основная таблица вакансий
        cur.execute("""
            CREATE TABLE vacancy_details (
                id SERIAL PRIMARY KEY,
                vacancy_id VARCHAR(50) UNIQUE NOT NULL,
                title TEXT NOT NULL,
                company VARCHAR(500),
                company_size VARCHAR(50),
                area VARCHAR(100),
                published_date TIMESTAMP,
                experience_raw VARCHAR(100),
                experience_level VARCHAR(50),
                role VARCHAR(50),
                domain VARCHAR(50),
                salary_from BIGINT,
                salary_to BIGINT,
                avg_salary BIGINT,
                tech_count INTEGER DEFAULT 0,
                skills_count INTEGER DEFAULT 0,
                fgos_competencies_count INTEGER DEFAULT 0,
                prof_competencies_count INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 4. Детальная таблица технологий
        cur.execute("""
            CREATE TABLE vacancy_technologies_detailed (
                id SERIAL PRIMARY KEY,
                vacancy_id VARCHAR(50) NOT NULL,
                technology VARCHAR(100) NOT NULL,
                frequency INTEGER DEFAULT 1,
                category VARCHAR(100),
                level VARCHAR(50),
                domain VARCHAR(50),
                fgos_competencies TEXT,
                prof_standards TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                FOREIGN KEY (vacancy_id) REFERENCES vacancy_details(vacancy_id) ON DELETE CASCADE
            )
        """)
        
        # Создаем индексы для OLAP
        indexes = [
            "CREATE INDEX idx_vac_role ON vacancy_details(role)",
            "CREATE INDEX idx_vac_domain ON vacancy_details(domain)",
            "CREATE INDEX idx_vac_exp_level ON vacancy_details(experience_level)",
            "CREATE INDEX idx_vac_salary ON vacancy_details(avg_salary)",
            "CREATE INDEX idx_tech_technology ON vacancy_technologies_detailed(technology)",
            "CREATE INDEX idx_tech_category ON vacancy_technologies_detailed(category)",
            "CREATE INDEX idx_fgos_direction ON fgos_competencies(direction_code)",
            "CREATE INDEX idx_fgos_competency ON fgos_competencies(competency_code)",
            "CREATE INDEX idx_otf_standard ON otf_td_standards(standard_code)"
        ]
        
        for index_sql in indexes:
            cur.execute(index_sql)
        
        conn.commit()
        print("✅ Финальные таблицы созданы")
        
    except Exception as e:
        print(f"❌ Ошибка создания таблиц: {e}")
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

def load_fgos_data():
    """Загрузка данных ФГОС"""
    csv_file = 'csv_files/fgos_competencies.csv'
    
    if not os.path.exists(csv_file):
        print(f"⚠️ Файл {csv_file} не найден")
        return False
    
    print(f"📚 Загрузка ФГОС из {csv_file}")
    
    df = pd.read_csv(csv_file)
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        loaded_count = 0
        for _, row in df.iterrows():
            try:
                cur.execute("""
                    INSERT INTO fgos_competencies (
                        direction_code, direction_name, competency_code, 
                        competency_name, competency_description, competency_type,
                        category, level_description
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (direction_code, competency_code) DO NOTHING
                """, (
                    clean_data(row.get('direction_code'), 'string', 20),
                    clean_data(row.get('direction_name'), 'string', 500),
                    clean_data(row.get('competency_code'), 'string', 20),
                    clean_data(row.get('competency_name'), 'string'),
                    clean_data(row.get('competency_description'), 'string'),
                    clean_data(row.get('competency_type'), 'string', 20),
                    clean_data(row.get('category'), 'string', 100),
                    clean_data(row.get('level_description'), 'string')
                ))
                loaded_count += 1
            except Exception as e:
                print(f"⚠️ Ошибка загрузки ФГОС строки: {e}")
                continue
        
        conn.commit()
        print(f"✅ ФГОС: загружено {loaded_count} записей")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка загрузки ФГОС: {e}")
        conn.rollback()
        return False
    finally:
        cur.close()
        conn.close()

def load_otf_td_data():
    """Загрузка данных профессиональных стандартов"""
    csv_file = 'csv_files/otf_td.csv'
    
    if not os.path.exists(csv_file):
        print(f"⚠️ Файл {csv_file} не найден")
        return False
    
    print(f"💼 Загрузка профстандартов из {csv_file}")
    
    df = pd.read_csv(csv_file)
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        loaded_count = 0
        for _, row in df.iterrows():
            try:
                cur.execute("""
                    INSERT INTO otf_td_standards (
                        standard_code, otf_code, otf_name, td_code, td_name
                    ) VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (standard_code, td_code) DO NOTHING
                """, (
                    clean_data(row.get('Стандарт'), 'string', 20),
                    clean_data(row.get('OTF_код'), 'string', 10),
                    clean_data(row.get('OTF_наименование'), 'string', 500),
                    clean_data(row.get('TD_код'), 'string', 20),
                    clean_data(row.get('TD_наименование'), 'string')
                ))
                loaded_count += 1
            except Exception as e:
                print(f"⚠️ Ошибка загрузки ОТФ строки: {e}")
                continue
        
        conn.commit()
        print(f"✅ Профстандарты: загружено {loaded_count} записей")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка загрузки профстандартов: {e}")
        conn.rollback()
        return False
    finally:
        cur.close()
        conn.close()

def load_hh_data():
    """Загрузка данных HH"""
    csv_dir = 'csv_files'
    
    # Находим свежие файлы HH
    vacancy_files = [f for f in os.listdir(csv_dir) if f.startswith('hh_vacancies_enhanced_')]
    tech_files = [f for f in os.listdir(csv_dir) if f.startswith('hh_technologies_detailed_')]
    
    if not vacancy_files or not tech_files:
        print("❌ Не найдены файлы HH данных в csv_files/")
        return False
    
    latest_vacancy_file = max(vacancy_files)
    latest_tech_file = max(tech_files)
    
    print(f"💼 Загрузка данных HH:")
    print(f"  📄 Вакансии: {latest_vacancy_file}")
    print(f"  🔧 Технологии: {latest_tech_file}")
    
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        # Загружаем вакансии
        vacancy_df = pd.read_csv(os.path.join(csv_dir, latest_vacancy_file))
        
        vacancy_loaded = 0
        for _, row in vacancy_df.iterrows():
            try:
                # Обработка даты
                published_date = row.get('published_date')
                if pd.notna(published_date) and published_date != '':
                    try:
                        published_date = pd.to_datetime(published_date).strftime('%Y-%m-%d %H:%M:%S')
                    except:
                        published_date = None
                else:
                    published_date = None
                
                cur.execute("""
                    INSERT INTO vacancy_details (
                        vacancy_id, title, company, company_size, area,
                        published_date, experience_raw, experience_level,
                        role, domain, salary_from, salary_to, avg_salary,
                        tech_count, skills_count, fgos_competencies_count, prof_competencies_count
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (vacancy_id) DO NOTHING
                """, (
                    clean_data(row.get('vacancy_id'), 'string', 50),
                    clean_data(row.get('title'), 'string'),
                    clean_data(row.get('company'), 'string', 500),
                    clean_data(row.get('company_size'), 'string', 50),
                    clean_data(row.get('area'), 'string', 100),
                    published_date,
                    clean_data(row.get('experience_raw'), 'string', 100),
                    clean_data(row.get('experience_level'), 'string', 50),
                    clean_data(row.get('role'), 'string', 50),
                    clean_data(row.get('domain'), 'string', 50),
                    clean_data(row.get('salary_from'), 'bigint'),
                    clean_data(row.get('salary_to'), 'bigint'),
                    clean_data(row.get('avg_salary'), 'bigint'),
                    clean_data(row.get('tech_count'), 'integer') or 0,
                    clean_data(row.get('skills_count'), 'integer') or 0,
                    clean_data(row.get('fgos_competencies_count'), 'integer') or 0,
                    clean_data(row.get('prof_competencies_count'), 'integer') or 0
                ))
                vacancy_loaded += 1
            except Exception as e:
                print(f"⚠️ Ошибка загрузки вакансии: {e}")
                continue
        
        print(f"✅ Вакансии: загружено {vacancy_loaded} записей")
        
        # Загружаем технологии
        tech_df = pd.read_csv(os.path.join(csv_dir, latest_tech_file))
        
        tech_loaded = 0
        for _, row in tech_df.iterrows():
            try:
                vacancy_id = clean_data(row.get('vacancy_id'), 'string', 50)
                
                # Проверяем существование вакансии
                cur.execute("SELECT 1 FROM vacancy_details WHERE vacancy_id = %s", (vacancy_id,))
                if not cur.fetchone():
                    continue
                
                cur.execute("""
                    INSERT INTO vacancy_technologies_detailed (
                        vacancy_id, technology, frequency, category, level,
                        domain, fgos_competencies, prof_standards
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    vacancy_id,
                    clean_data(row.get('technology'), 'string', 100),
                    clean_data(row.get('frequency'), 'integer') or 1,
                    clean_data(row.get('category'), 'string', 100),
                    clean_data(row.get('level'), 'string', 50),
                    clean_data(row.get('domain'), 'string', 50),
                    clean_data(row.get('fgos_competencies'), 'string'),
                    clean_data(row.get('prof_standards'), 'string')
                ))
                tech_loaded += 1
            except Exception as e:
                print(f"⚠️ Ошибка загрузки технологии: {e}")
                continue
        
        print(f"✅ Технологии: загружено {tech_loaded} записей")
        
        conn.commit()
        return True
        
    except Exception as e:
        print(f"❌ Ошибка загрузки HH данных: {e}")
        conn.rollback()
        return False
    finally:
        cur.close()
        conn.close()

def create_olap_views():
    """Создание представлений для OLAP анализа"""
    conn = get_connection()
    cur = conn.cursor()
    
    print("📊 Создание OLAP представлений...")
    
    try:
        # 1. Основное представление для анализа компетенций
        cur.execute("""
            CREATE OR REPLACE VIEW olap_competency_analysis AS
            SELECT 
                vd.vacancy_id,
                vd.title,
                vd.company,
                vd.role,
                vd.domain,
                vd.experience_level,
                vd.avg_salary,
                vd.area,
                
                vtd.technology,
                vtd.category as tech_category,
                vtd.level as tech_level,
                vtd.frequency,
                
                -- Извлекаем ФГОС компетенции из строки
                CASE WHEN vtd.fgos_competencies IS NOT NULL 
                     THEN string_to_array(vtd.fgos_competencies, ',')
                     ELSE ARRAY[]::text[] 
                END as fgos_competencies_array,
                
                -- Извлекаем профстандарты из строки  
                CASE WHEN vtd.prof_standards IS NOT NULL
                     THEN string_to_array(vtd.prof_standards, ',')
                     ELSE ARRAY[]::text[]
                END as prof_standards_array,
                
                -- Диапазоны зарплат для группировки
                CASE 
                    WHEN vd.avg_salary IS NULL THEN 'Не указана'
                    WHEN vd.avg_salary < 100000 THEN 'До 100к'
                    WHEN vd.avg_salary < 200000 THEN '100-200к'
                    WHEN vd.avg_salary < 300000 THEN '200-300к'
                    ELSE '300к+'
                END as salary_range,
                
                -- Извлекаем год и месяц
                EXTRACT(YEAR FROM vd.published_date) as publish_year,
                EXTRACT(MONTH FROM vd.published_date) as publish_month
                
            FROM vacancy_details vd
            LEFT JOIN vacancy_technologies_detailed vtd ON vd.vacancy_id = vtd.vacancy_id
        """)
        
        # 2. Агрегированное представление по технологиям
        cur.execute("""
            CREATE OR REPLACE VIEW tech_market_summary AS
            SELECT 
                technology,
                tech_category,
                COUNT(DISTINCT vacancy_id) as vacancy_count,
                SUM(frequency) as total_mentions,
                AVG(avg_salary) as avg_salary,
                COUNT(DISTINCT company) as company_count,
                COUNT(DISTINCT role) as role_count,
                
                -- Топ роль для технологии
                MODE() WITHIN GROUP (ORDER BY role) as top_role,
                
                -- Топ уровень опыта
                MODE() WITHIN GROUP (ORDER BY experience_level) as top_experience_level
                
            FROM olap_competency_analysis
            WHERE technology IS NOT NULL
            GROUP BY technology, tech_category
        """)
        
        # 3. Куб для анализа роль × технология × зарплата
        cur.execute("""
            CREATE OR REPLACE VIEW role_tech_salary_cube AS
            SELECT 
                role,
                technology,
                salary_range,
                COUNT(*) as vacancy_count,
                AVG(avg_salary) as avg_salary,
                MIN(avg_salary) as min_salary,
                MAX(avg_salary) as max_salary
            FROM olap_competency_analysis
            WHERE role IS NOT NULL AND technology IS NOT NULL
            GROUP BY role, technology, salary_range
        """)
        
        conn.commit()
        print("✅ OLAP представления созданы")
        
    except Exception as e:
        print(f"❌ Ошибка создания представлений: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def show_final_summary():
    """Финальная сводка"""
    conn = get_connection()
    cur = conn.cursor()
    
    print(f"\n📊 ФИНАЛЬНАЯ СВОДКА ДАННЫХ")
    print("=" * 60)
    
    queries = [
        ("📚 ФГОС компетенции", "SELECT COUNT(*) FROM fgos_competencies"),
        ("💼 Профстандарты (ОТФ/ТД)", "SELECT COUNT(*) FROM otf_td_standards"),
        ("🏢 Вакансии", "SELECT COUNT(*) FROM vacancy_details"),
        ("🔧 Технологии (детально)", "SELECT COUNT(*) FROM vacancy_technologies_detailed"),
        ("💰 Вакансии с зарплатами", "SELECT COUNT(*) FROM vacancy_details WHERE avg_salary IS NOT NULL"),
        ("🌟 Уникальных технологий", "SELECT COUNT(DISTINCT technology) FROM vacancy_technologies_detailed"),
        ("🏭 Уникальных компаний", "SELECT COUNT(DISTINCT company) FROM vacancy_details WHERE company IS NOT NULL")
    ]
    
    for name, query in queries:
        try:
            cur.execute(query)
            result = cur.fetchone()[0]
            print(f"  {name}: {result:,}")
        except Exception as e:
            print(f"  ❌ {name}: Ошибка - {e}")
    
    # Топ технологии
    print(f"\n🔧 ТОП-10 ТЕХНОЛОГИЙ:")
    cur.execute("""
        SELECT technology, vacancy_count, avg_salary
        FROM tech_market_summary
        ORDER BY vacancy_count DESC
        LIMIT 10
    """)
    
    for i, (tech, count, salary) in enumerate(cur.fetchall(), 1):
        salary_str = f"{salary:,.0f} руб." if salary else "н/д"
        print(f"  {i:2d}. {tech}: {count} вакансий (ср. {salary_str})")
    
    cur.close()
    conn.close()

def main():
    """Главная функция загрузки"""
    print("🚀 ФИНАЛЬНАЯ ЗАГРУЗКА ДАННЫХ ДЛЯ OLAP АНАЛИЗА")
    print("=" * 70)
    
    try:
        # 1. Создаем таблицы
        create_final_tables()
        
        # 2. Загружаем ФГОС
        if not load_fgos_data():
            print("⚠️ Проблемы с загрузкой ФГОС, но продолжаем...")
        
        # 3. Загружаем профстандарты
        if not load_otf_td_data():
            print("⚠️ Проблемы с загрузкой профстандартов, но продолжаем...")
        
        # 4. Загружаем данные HH
        if not load_hh_data():
            print("❌ Критическая ошибка с данными HH")
            return False
        
        # 5. Создаем OLAP представления
        create_olap_views()
        
        # 6. Показываем итоги
        show_final_summary()
        
        print(f"\n✅ ФИНАЛЬНАЯ ЗАГРУЗКА ЗАВЕРШЕНА!")
        print("🎯 Готово для:")
        print("  • Настройки ClickHouse")
        print("  • Создания OLAP кубов")
        print("  • GROUP BY CUBE запросов")
        print("  • Визуализации в Grafana/DataLens")
        
        return True
        
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    main()