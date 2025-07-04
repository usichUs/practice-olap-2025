import psycopg2
import pandas as pd
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
        return None

def get_all_tables():
    """Получение списка всех таблиц в базе"""
    conn = get_connection()
    if not conn:
        return []
    
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)
        tables = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()
        return tables
    except Exception as e:
        print(f"❌ Ошибка получения списка таблиц: {e}")
        return []

def show_table_summary(table_name, conn, show_samples=True, sample_count=3):
    """Показать краткую сводку по таблице"""
    print(f"\n📊 {table_name.upper()}:")
    print("-" * 60)
    
    try:
        # Получаем общую информацию
        count_query = f"SELECT COUNT(*) FROM {table_name}"
        df_count = pd.read_sql_query(count_query, conn)
        total_records = df_count.iloc[0, 0]
        
        if total_records == 0:
            print("  📭 Нет данных")
            return
        
        # Получаем структуру таблицы
        columns_query = f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_name = '{table_name}' 
            ORDER BY ordinal_position
        """
        columns_df = pd.read_sql_query(columns_query, conn)
        
        print(f"  📈 Всего записей: {total_records:,}")
        print(f"  📋 Столбцов: {len(columns_df)}")
        
        # Показываем структуру (кратко)
        if len(columns_df) <= 8:
            column_list = list(columns_df['column_name'])
        else:
            column_list = list(columns_df['column_name'][:6]) + ['...']
        print(f"  🗃️  Поля: {', '.join(column_list)}")
        
        # Показываем образцы данных если нужно
        if show_samples and total_records > 0:
            sample_query = f"SELECT * FROM {table_name} LIMIT {sample_count}"
            sample_df = pd.read_sql_query(sample_query, conn)
            
            print(f"\n  📝 Первые {min(sample_count, len(sample_df))} записи:")
            
            for i, row in sample_df.iterrows():
                # Ограничиваем длину значений для красивого вывода
                row_dict = {}
                for col, val in row.items():
                    if pd.isna(val):
                        row_dict[col] = 'NULL'
                    elif isinstance(val, str) and len(val) > 50:
                        row_dict[col] = val[:47] + '...'
                    else:
                        row_dict[col] = val
                
                key_fields = list(row_dict.items())[:3]  # Показываем первые 3 поля
                print(f"    {i+1:2d}. " + " | ".join([f"{k}:{v}" for k, v in key_fields]))
            
            if total_records > sample_count:
                print(f"       ... и еще {total_records - sample_count:,} записей")
                
    except Exception as e:
        print(f"  ❌ Ошибка: {e}")

def show_enhanced_analytics():
    """Улучшенная аналитика по данным"""
    conn = get_connection()
    if not conn:
        return
    
    print(f"\n📈 АНАЛИТИКА ПО ДАННЫМ")
    print("=" * 80)
    
    analytics_queries = {
        "🔧 Топ-10 технологий": {
            "query": """
                SELECT technology, COUNT(*) as vacancy_count, SUM(frequency) as total_mentions
                FROM vacancy_technologies_detailed 
                GROUP BY technology 
                ORDER BY vacancy_count DESC 
                LIMIT 10
            """,
            "format": lambda row: f"{row['technology']} ({row['vacancy_count']} вакансий, {row['total_mentions']} упоминаний)"
        },
        
        "👥 Распределение ролей": {
            "query": """
                SELECT role, COUNT(*) as count, AVG(avg_salary) as avg_salary
                FROM vacancy_details 
                WHERE role IS NOT NULL 
                GROUP BY role 
                ORDER BY count DESC 
                LIMIT 8
            """,
            "format": lambda row: f"{row['role']}: {row['count']} вакансий (ср. зарплата: {row['avg_salary']:,.0f} руб.)" if row['avg_salary'] else f"{row['role']}: {row['count']} вакансий"
        },
        
        "🏢 Топ компании": {
            "query": """
                SELECT company, COUNT(*) as vacancy_count, AVG(avg_salary) as avg_salary
                FROM vacancy_details 
                WHERE company IS NOT NULL 
                GROUP BY company 
                ORDER BY vacancy_count DESC 
                LIMIT 10
            """,
            "format": lambda row: f"{row['company']}: {row['vacancy_count']} вакансий"
        },
        
        "📚 ФГОС компетенции по типам": {
            "query": """
                SELECT competency_type, COUNT(*) as count
                FROM fgos_competencies
                WHERE competency_type IS NOT NULL
                GROUP BY competency_type
                ORDER BY count DESC
            """,
            "format": lambda row: f"{row['competency_type']}: {row['count']} компетенций"
        },
        
        "💼 Профстандарты по кодам": {
            "query": """
                SELECT standard_code, COUNT(*) as otf_count
                FROM otf_td_standards
                GROUP BY standard_code
                ORDER BY otf_count DESC
            """,
            "format": lambda row: f"{row['standard_code']}: {row['otf_count']} ОТФ/ТД"
        },
        
        "🔗 Технологии с компетенциями ФГОС": {
            "query": """
                SELECT 
                    COUNT(*) as total_tech_records,
                    COUNT(CASE WHEN fgos_competencies IS NOT NULL THEN 1 END) as with_fgos,
                    ROUND(COUNT(CASE WHEN fgos_competencies IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 1) as fgos_coverage
                FROM vacancy_technologies_detailed
            """,
            "format": lambda row: f"Всего записей: {row['total_tech_records']}, с ФГОС: {row['with_fgos']} ({row['fgos_coverage']}%)"
        },
        
        "🔗 Технологии с профстандартами": {
            "query": """
                SELECT 
                    COUNT(*) as total_tech_records,
                    COUNT(CASE WHEN prof_standards IS NOT NULL THEN 1 END) as with_prof,
                    ROUND(COUNT(CASE WHEN prof_standards IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 1) as prof_coverage
                FROM vacancy_technologies_detailed
            """,
            "format": lambda row: f"Всего записей: {row['total_tech_records']}, с профстандартами: {row['with_prof']} ({row['prof_coverage']}%)"
        }
    }
    
    for title, query_info in analytics_queries.items():
        print(f"\n{title}:")
        print("-" * 50)
        
        try:
            df = pd.read_sql_query(query_info["query"], conn)
            
            if len(df) == 0:
                print("  📭 Нет данных")
            else:
                for i, row in df.iterrows():
                    formatted_line = query_info["format"](row)
                    print(f"  {i+1:2d}. {formatted_line}")
                    
        except Exception as e:
            print(f"  ❌ Ошибка: {e}")
    
    conn.close()

def show_olap_readiness():
    """Проверка готовности к OLAP анализу"""
    conn = get_connection()
    if not conn:
        return
    
    print(f"\n🎯 ГОТОВНОСТЬ К OLAP АНАЛИЗУ")
    print("=" * 80)
    
    try:
        # Проверяем представления
        cur = conn.cursor()
        cur.execute("""
            SELECT table_name 
            FROM information_schema.views 
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)
        views = [row[0] for row in cur.fetchall()]
        
        expected_views = ['olap_competency_analysis', 'tech_market_summary', 'role_tech_salary_cube']
        
        print("📊 OLAP представления:")
        for view in expected_views:
            if view in views:
                # Проверяем количество записей
                cur.execute(f"SELECT COUNT(*) FROM {view}")
                count = cur.fetchone()[0]
                print(f"  ✅ {view}: {count:,} записей")
            else:
                print(f"  ❌ {view}: отсутствует")
        
        # Проверяем возможность CUBE запросов
        print(f"\n🧊 Тест GROUP BY CUBE:")
        try:
            cur.execute("""
                SELECT 
                    role, 
                    tech_category,
                    COUNT(*) as count
                FROM olap_competency_analysis 
                WHERE role IS NOT NULL AND tech_category IS NOT NULL
                GROUP BY CUBE(role, tech_category)
                LIMIT 5
            """)
            cube_results = cur.fetchall()
            print(f"  ✅ GROUP BY CUBE работает ({len(cube_results)} результатов)")
        except Exception as e:
            print(f"  ❌ GROUP BY CUBE: {e}")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка проверки OLAP: {e}")

def show_data_quality_report():
    """Отчет о качестве данных"""
    conn = get_connection()
    if not conn:
        return
    
    print(f"\n🔍 ОТЧЕТ О КАЧЕСТВЕ ДАННЫХ")
    print("=" * 80)
    
    quality_checks = [
        {
            "name": "Вакансии с зарплатами",
            "query": """
                SELECT 
                    COUNT(*) as total,
                    COUNT(avg_salary) as with_salary,
                    ROUND(COUNT(avg_salary) * 100.0 / COUNT(*), 1) as percentage
                FROM vacancy_details
            """,
            "format": lambda row: f"Всего: {row['total']}, с зарплатой: {row['with_salary']} ({row['percentage']}%)"
        },
        {
            "name": "Вакансии с определенными ролями",
            "query": """
                SELECT 
                    COUNT(*) as total,
                    COUNT(role) as with_role,
                    ROUND(COUNT(role) * 100.0 / COUNT(*), 1) as percentage
                FROM vacancy_details
            """,
            "format": lambda row: f"Всего: {row['total']}, с ролью: {row['with_role']} ({row['percentage']}%)"
        },
        {
            "name": "Связанность технологий",
            "query": """
                SELECT 
                    COUNT(DISTINCT vtd.technology) as unique_technologies,
                    COUNT(*) as total_technology_records,
                    ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT vtd.vacancy_id), 1) as avg_tech_per_vacancy
                FROM vacancy_technologies_detailed vtd
            """,
            "format": lambda row: f"Уникальных технологий: {row['unique_technologies']}, в среднем {row['avg_tech_per_vacancy']} на вакансию"
        },
        {
            "name": "Покрытие компетенциями",
            "query": """
                SELECT 
                    COUNT(DISTINCT technology) as total_technologies,
                    COUNT(DISTINCT CASE WHEN fgos_competencies IS NOT NULL THEN technology END) as with_fgos,
                    COUNT(DISTINCT CASE WHEN prof_standards IS NOT NULL THEN technology END) as with_prof
                FROM vacancy_technologies_detailed
            """,
            "format": lambda row: f"Технологий: {row['total_technologies']}, с ФГОС: {row['with_fgos']}, с профстандартами: {row['with_prof']}"
        }
    ]
    
    for check in quality_checks:
        try:
            df = pd.read_sql_query(check["query"], conn)
            if len(df) > 0:
                result = check["format"](df.iloc[0])
                print(f"  ✅ {check['name']}: {result}")
        except Exception as e:
            print(f"  ❌ {check['name']}: Ошибка - {e}")
    
    conn.close()

def main():
    """Главная функция с меню"""
    print("🚀 ПРОВЕРКА ФИНАЛЬНЫХ ДАННЫХ ДЛЯ OLAP")
    print(f"⏰ Время запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    # Получаем список таблиц
    tables = get_all_tables()
    
    if not tables:
        print("❌ Не удалось получить список таблиц")
        return
    
    print(f"📚 Найдено таблиц: {len(tables)}")
    
    # Актуальные таблицы для OLAP
    final_tables = ['fgos_competencies', 'otf_td_standards', 'vacancy_details', 'vacancy_technologies_detailed']
    other_tables = [t for t in tables if t not in final_tables]
    
    conn = get_connection()
    if not conn:
        return
    
    # Показываем финальные таблицы
    print(f"\n🎯 ФИНАЛЬНЫЕ ТАБЛИЦЫ ДЛЯ OLAP:")
    for table in final_tables:
        if table in tables:
            show_table_summary(table, conn, show_samples=True, sample_count=3)
        else:
            print(f"\n❌ {table.upper()}: ОТСУТСТВУЕТ")
    
    # Показываем остальные таблицы если есть
    if other_tables:
        print(f"\n📋 ДОПОЛНИТЕЛЬНЫЕ ТАБЛИЦЫ:")
        for table in other_tables:
            show_table_summary(table, conn, show_samples=False)
    
    conn.close()
    
    # Показываем аналитику
    show_enhanced_analytics()
    
    # Проверяем готовность к OLAP
    show_olap_readiness()
    
    # Показываем отчет о качестве
    show_data_quality_report()
    
    print(f"\n✅ Проверка завершена!")

if __name__ == "__main__":
    main()