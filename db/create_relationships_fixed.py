import psycopg2
import sys

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
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        return conn
    except Exception as e:
        print(f"❌ Ошибка подключения к БД: {e}")
        sys.exit(1)

def create_relationships_table():
    """Создание таблицы связей технологий"""
    conn = get_connection()
    cur = conn.cursor()
    
    print("🔧 Создаем таблицу связей технологий...")
    
    try:
        # Удаляем старую таблицу если есть
        cur.execute("DROP TABLE IF EXISTS technology_relationships CASCADE")
        
        # Создаем новую таблицу связей
        cur.execute("""
            CREATE TABLE technology_relationships (
                id SERIAL PRIMARY KEY,
                technology_1 VARCHAR(100) NOT NULL,
                technology_2 VARCHAR(100) NOT NULL,
                relationship_type VARCHAR(50) NOT NULL,
                strength DECIMAL(3,2) DEFAULT 0.5,
                frequency INTEGER DEFAULT 1,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                UNIQUE(technology_1, technology_2, relationship_type),
                
                CONSTRAINT valid_relationship_type 
                CHECK (relationship_type IN ('cooccurrence', 'complementary', 'same_category', 'prerequisite', 'alternative'))
            )
        """)
        
        # Создаем индексы
        cur.execute("CREATE INDEX idx_tech_rel_tech1 ON technology_relationships(technology_1)")
        cur.execute("CREATE INDEX idx_tech_rel_tech2 ON technology_relationships(technology_2)")
        cur.execute("CREATE INDEX idx_tech_rel_type ON technology_relationships(relationship_type)")
        cur.execute("CREATE INDEX idx_tech_rel_strength ON technology_relationships(strength)")
        
        conn.commit()
        print("✅ Таблица связей технологий создана")
        
    except Exception as e:
        print(f"❌ Ошибка создания таблицы: {e}")
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

def create_cooccurrence_relationships():
    """Создание связей на основе совместного появления технологий в вакансиях"""
    conn = get_connection()
    cur = conn.cursor()
    
    print("🔗 Создаем связи на основе совместного появления...")
    
    try:
        # Находим технологии, которые часто встречаются вместе
        cur.execute("""
            INSERT INTO technology_relationships (technology_1, technology_2, relationship_type, strength, frequency, description)
            SELECT 
                t1.technology as tech1,
                t2.technology as tech2,
                'cooccurrence' as relationship_type,
                LEAST(1.0, COUNT(*) / 50.0) as strength,  -- Нормализуем силу связи
                COUNT(*) as frequency,
                'Технологии часто используются вместе в ' || COUNT(*) || ' вакансиях'
            FROM vacancy_technologies_detailed t1
            JOIN vacancy_technologies_detailed t2 ON t1.vacancy_id = t2.vacancy_id
            WHERE t1.technology < t2.technology  -- Избегаем дублирования и само-связей
                AND t1.technology IS NOT NULL 
                AND t2.technology IS NOT NULL
                AND t1.technology != t2.technology
            GROUP BY t1.technology, t2.technology
            HAVING COUNT(*) >= 3  -- Минимум 3 совместных появления
            ON CONFLICT (technology_1, technology_2, relationship_type) DO NOTHING
        """)
        
        cooccurrence_count = cur.rowcount
        print(f"✅ Создано {cooccurrence_count} связей совместного появления")
        
        conn.commit()
        
    except Exception as e:
        print(f"❌ Ошибка создания связей совместного появления: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def create_category_relationships():
    """Создание связей внутри категорий технологий"""
    conn = get_connection()
    cur = conn.cursor()
    
    print("🔗 Создаем связи внутри категорий...")
    
    try:
        # Связи между технологиями одной категории
        cur.execute("""
            INSERT INTO technology_relationships (technology_1, technology_2, relationship_type, strength, description)
            SELECT DISTINCT
                t1.technology as tech1,
                t2.technology as tech2,
                'same_category' as relationship_type,
                0.4 as strength,  -- Средняя связь по категории
                'Обе технологии относятся к категории: ' || COALESCE(t1.category, 'Unknown')
            FROM vacancy_technologies_detailed t1
            JOIN vacancy_technologies_detailed t2 ON t1.category = t2.category
            WHERE t1.technology < t2.technology
                AND t1.category IS NOT NULL
                AND t2.category IS NOT NULL
                AND t1.category != ''
                AND t1.technology != t2.technology
            ON CONFLICT (technology_1, technology_2, relationship_type) DO NOTHING
        """)
        
        category_count = cur.rowcount
        print(f"✅ Создано {category_count} связей по категориям")
        
        conn.commit()
        
    except Exception as e:
        print(f"❌ Ошибка создания связей по категориям: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def create_predefined_relationships():
    """Создание предопределенных связей дополняющих технологий"""
    conn = get_connection()
    cur = conn.cursor()
    
    print("🔗 Создаем предопределенные связи...")
    
    try:
        # Определяем известные связи технологий
        predefined_relationships = [
            # Frontend стек
            ('React', 'JavaScript', 'complementary', 0.9, 'React основан на JavaScript'),
            ('Vue', 'JavaScript', 'complementary', 0.9, 'Vue основан на JavaScript'),
            ('Angular', 'TypeScript', 'complementary', 0.8, 'Angular использует TypeScript'),
            ('HTML', 'CSS', 'complementary', 0.9, 'HTML и CSS работают вместе'),
            ('React', 'Redux', 'complementary', 0.7, 'Redux часто используется с React'),
            
            # Backend стек
            ('Python', 'Django', 'complementary', 0.8, 'Django - фреймворк Python'),
            ('Python', 'Flask', 'complementary', 0.7, 'Flask - фреймворк Python'),
            ('JavaScript', 'Node.js', 'complementary', 0.8, 'Node.js выполняет JavaScript'),
            ('Java', 'Spring', 'complementary', 0.8, 'Spring - фреймворк Java'),
            
            # Базы данных
            ('SQL', 'PostgreSQL', 'complementary', 0.8, 'PostgreSQL использует SQL'),
            ('SQL', 'MySQL', 'complementary', 0.8, 'MySQL использует SQL'),
            ('Python', 'PostgreSQL', 'complementary', 0.7, 'Python часто работает с PostgreSQL'),
            
            # DevOps
            ('Docker', 'Kubernetes', 'complementary', 0.8, 'Kubernetes оркестрирует Docker'),
            ('Git', 'GitHub', 'complementary', 0.8, 'GitHub использует Git'),
            ('Docker', 'CI/CD', 'complementary', 0.7, 'Docker используется в CI/CD'),
            
            # Альтернативы
            ('React', 'Vue', 'alternative', 0.6, 'React и Vue - альтернативные фреймворки'),
            ('PostgreSQL', 'MySQL', 'alternative', 0.5, 'PostgreSQL и MySQL - альтернативные СУБД'),
            ('Docker', 'Podman', 'alternative', 0.7, 'Podman - альтернатива Docker'),
            
            # Пререквизиты
            ('JavaScript', 'TypeScript', 'prerequisite', 0.7, 'TypeScript расширяет JavaScript'),
            ('HTML', 'React', 'prerequisite', 0.6, 'Знание HTML полезно для React'),
            ('SQL', 'Database', 'prerequisite', 0.8, 'SQL нужен для работы с БД'),
        ]
        
        created_count = 0
        
        for tech1, tech2, rel_type, strength, description in predefined_relationships:
            try:
                # Проверяем, что обе технологии существуют в наших данных
                cur.execute("""
                    SELECT COUNT(DISTINCT technology) FROM vacancy_technologies_detailed 
                    WHERE technology IN (%s, %s)
                """, (tech1, tech2))
                
                if cur.fetchone()[0] == 2:  # Обе технологии найдены
                    # Сортируем технологии для избежания дублирования
                    sorted_tech1, sorted_tech2 = sorted([tech1, tech2])
                    
                    cur.execute("""
                        INSERT INTO technology_relationships (technology_1, technology_2, relationship_type, strength, description)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (technology_1, technology_2, relationship_type) DO NOTHING
                    """, (sorted_tech1, sorted_tech2, rel_type, strength, description))
                    
                    if cur.rowcount > 0:
                        created_count += 1
                        print(f"  ✅ {tech1} ↔ {tech2} ({rel_type})")
                        
            except Exception as e:
                print(f"⚠️ Ошибка добавления связи {tech1}-{tech2}: {e}")
                continue
        
        print(f"✅ Создано {created_count} предопределенных связей")
        
        conn.commit()
        
    except Exception as e:
        print(f"❌ Ошибка создания предопределенных связей: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def create_analysis_views():
    """Создание представлений для анализа связей"""
    conn = get_connection()
    cur = conn.cursor()
    
    print("📊 Создаем представления для анализа связей...")
    
    try:
        # Представление связей с дополнительной информацией
        cur.execute("""
            CREATE OR REPLACE VIEW technology_relationships_extended AS
            SELECT 
                tr.id,
                tr.technology_1,
                tr.technology_2,
                tr.relationship_type,
                tr.strength,
                tr.frequency,
                tr.description,
                
                -- Информация о первой технологии
                t1_stats.vacancy_count as tech1_vacancy_count,
                t1_stats.category as tech1_category,
                
                -- Информация о второй технологии
                t2_stats.vacancy_count as tech2_vacancy_count,
                t2_stats.category as tech2_category,
                
                tr.created_at
            FROM technology_relationships tr
            LEFT JOIN (
                SELECT 
                    technology,
                    COUNT(DISTINCT vacancy_id) as vacancy_count,
                    MODE() WITHIN GROUP (ORDER BY category) as category
                FROM vacancy_technologies_detailed 
                GROUP BY technology
            ) t1_stats ON tr.technology_1 = t1_stats.technology
            LEFT JOIN (
                SELECT 
                    technology,
                    COUNT(DISTINCT vacancy_id) as vacancy_count,
                    MODE() WITHIN GROUP (ORDER BY category) as category
                FROM vacancy_technologies_detailed 
                GROUP BY technology
            ) t2_stats ON tr.technology_2 = t2_stats.technology
        """)
        
        # Представление топ связей
        cur.execute("""
            CREATE OR REPLACE VIEW top_technology_relationships AS
            SELECT 
                technology_1,
                technology_2,
                relationship_type,
                strength,
                frequency,
                description,
                tech1_category,
                tech2_category
            FROM technology_relationships_extended
            ORDER BY strength DESC, frequency DESC
        """)
        
        # Представление статистики по технологиям
        cur.execute("""
            CREATE OR REPLACE VIEW technology_network_stats AS
            SELECT 
                technology,
                COUNT(*) as total_relationships,
                COUNT(CASE WHEN relationship_type = 'cooccurrence' THEN 1 END) as cooccurrence_links,
                COUNT(CASE WHEN relationship_type = 'complementary' THEN 1 END) as complementary_links,
                COUNT(CASE WHEN relationship_type = 'same_category' THEN 1 END) as category_links,
                AVG(strength) as avg_relationship_strength,
                MAX(strength) as max_relationship_strength
            FROM (
                SELECT technology_1 as technology, relationship_type, strength FROM technology_relationships
                UNION ALL
                SELECT technology_2 as technology, relationship_type, strength FROM technology_relationships
            ) all_relationships
            GROUP BY technology
            ORDER BY total_relationships DESC
        """)
        
        conn.commit()
        print("✅ Представления для анализа связей созданы")
        
    except Exception as e:
        print(f"❌ Ошибка создания представлений: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def show_relationships_summary():
    """Показать сводку по связям"""
    conn = get_connection()
    cur = conn.cursor()
    
    print(f"\n📊 СВОДКА ПО СВЯЗЯМ ТЕХНОЛОГИЙ")
    print("=" * 60)
    
    try:
        # Общее количество связей
        cur.execute("SELECT COUNT(*) FROM technology_relationships")
        total_relationships = cur.fetchone()[0]
        print(f"🔗 Всего связей: {total_relationships:,}")
        
        # По типам связей
        cur.execute("""
            SELECT relationship_type, COUNT(*), AVG(strength)
            FROM technology_relationships 
            GROUP BY relationship_type
            ORDER BY COUNT(*) DESC
        """)
        print(f"\n📈 По типам связей:")
        for row in cur.fetchall():
            print(f"  • {row[0]}: {row[1]:,} связей (средняя сила: {row[2]:.2f})")
        
        # Топ-10 самых связанных технологий
        cur.execute("""
            SELECT technology, total_relationships, cooccurrence_links, complementary_links
            FROM technology_network_stats
            ORDER BY total_relationships DESC
            LIMIT 10
        """)
        print(f"\n🌟 Топ-10 самых связанных технологий:")
        for i, row in enumerate(cur.fetchall(), 1):
            print(f"  {i:2d}. {row[0]}: {row[1]} связей ({row[2]} совм., {row[3]} комп.)")
        
        # Топ-10 сильных связей
        cur.execute("""
            SELECT technology_1, technology_2, relationship_type, strength, frequency
            FROM technology_relationships
            ORDER BY strength DESC, frequency DESC
            LIMIT 10
        """)
        print(f"\n💪 Топ-10 сильных связей:")
        for i, row in enumerate(cur.fetchall(), 1):
            freq_str = f", частота: {row[4]}" if row[4] and row[4] > 1 else ""
            print(f"  {i:2d}. {row[0]} ↔ {row[1]} ({row[2]}, сила: {row[3]:.2f}{freq_str})")
        
        # Статистика по категориям
        cur.execute("""
            SELECT 
                CASE 
                    WHEN tech1_category = tech2_category THEN 'Внутри категории'
                    ELSE 'Между категориями'
                END as relationship_scope,
                COUNT(*) as count
            FROM technology_relationships_extended
            WHERE tech1_category IS NOT NULL AND tech2_category IS NOT NULL
            GROUP BY 
                CASE 
                    WHEN tech1_category = tech2_category THEN 'Внутри категории'
                    ELSE 'Между категориями'
                END
        """)
        print(f"\n🏷️ Связи по категориям:")
        for row in cur.fetchall():
            print(f"  • {row[0]}: {row[1]:,} связей")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
    finally:
        cur.close()
        conn.close()

def main():
    """Главная функция создания связей"""
    print("🚀 СОЗДАНИЕ СВЯЗЕЙ ТЕХНОЛОГИЙ ДЛЯ НОВОЙ СТРУКТУРЫ БД")
    print("=" * 60)
    
    try:
        # 1. Создаем таблицу связей
        create_relationships_table()
        
        # 2. Создаем связи совместного появления
        create_cooccurrence_relationships()
        
        # 3. Создаем связи по категориям
        create_category_relationships()
        
        # 4. Создаем предопределенные связи
        create_predefined_relationships()
        
        # 5. Создаем представления для анализа
        create_analysis_views()
        
        # 6. Показываем сводку
        show_relationships_summary()
        
        print(f"\n✅ СОЗДАНИЕ СВЯЗЕЙ ЗАВЕРШЕНО!")
        print("🎯 Связи готовы для анализа и визуализации!")
        print("\n🔍 Проверьте результат:")
        print("   python3 db/check_data.py")
        
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    main()