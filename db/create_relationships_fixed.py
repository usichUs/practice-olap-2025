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
        return psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        print(f"❌ Ошибка подключения к БД: {e}")
        sys.exit(1)

def create_relationships_table():
    """Создаем таблицу связей компетенций"""
    conn = get_connection()
    cur = conn.cursor()
    
    print("🔧 Создаем таблицу связей компетенций...")
    
    try:
        # Проверяем, существует ли таблица
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'competency_relationships'
            )
        """)
        
        table_exists = cur.fetchone()[0]
        
        if not table_exists:
            cur.execute("""
                CREATE TABLE competency_relationships (
                    id SERIAL PRIMARY KEY,
                    competency_a_id INTEGER NOT NULL,
                    competency_b_id INTEGER NOT NULL,
                    relationship_type VARCHAR(50) NOT NULL,
                    description TEXT,
                    strength FLOAT DEFAULT 1.0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    
                    FOREIGN KEY (competency_a_id) REFERENCES competencies(id) ON DELETE CASCADE,
                    FOREIGN KEY (competency_b_id) REFERENCES competencies(id) ON DELETE CASCADE,
                    
                    UNIQUE(competency_a_id, competency_b_id, relationship_type),
                    
                    CONSTRAINT valid_relationship_type 
                    CHECK (relationship_type IN ('complement', 'alternative', 'prerequisite', 'popular_together', 'same_category'))
                )
            """)
            
            # Создаем индексы для производительности
            cur.execute("CREATE INDEX idx_comp_rel_a ON competency_relationships(competency_a_id)")
            cur.execute("CREATE INDEX idx_comp_rel_b ON competency_relationships(competency_b_id)")
            cur.execute("CREATE INDEX idx_comp_rel_type ON competency_relationships(relationship_type)")
            
            conn.commit()
            print("✅ Таблица competency_relationships создана")
        else:
            print("✅ Таблица competency_relationships уже существует")
            # Очищаем существующие данные
            cur.execute("DELETE FROM competency_relationships")
            conn.commit()
            print("🧹 Очищены существующие связи")
    
    except Exception as e:
        print(f"❌ Ошибка создания таблицы: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def create_technology_relationships():
    """Создаем связи между технологиями на основе совместного использования"""
    conn = get_connection()
    cur = conn.cursor()
    
    print("\n🔗 Создаем связи между технологиями...")
    
    try:
        # Определяем связи на основе схожих технологий
        relationships = [
            # Frontend технологии
            ('React', 'JavaScript', 'complement', 'React использует JavaScript'),
            ('React', 'TypeScript', 'complement', 'React часто используется с TypeScript'),
            ('Vue', 'JavaScript', 'complement', 'Vue использует JavaScript'),
            ('Angular', 'TypeScript', 'complement', 'Angular использует TypeScript'),
            
            # Backend технологии
            ('Python', 'PostgreSQL', 'complement', 'Python часто работает с PostgreSQL'),
            ('Java', 'SQL', 'complement', 'Java приложения используют SQL'),
            
            # DevOps связи
            ('Docker', 'CI/CD', 'complement', 'Docker используется в CI/CD'),
            ('Git', 'CI/CD', 'complement', 'Git интегрируется с CI/CD'),
            
            # Архитектурные связи
            ('REST', 'JavaScript', 'complement', 'REST API потребляются JavaScript'),
            ('REST', 'Python', 'complement', 'REST API разрабатываются на Python'),
            
            # Облачные технологии
            ('AWS', 'Docker', 'complement', 'AWS поддерживает Docker'),
            ('Azure', 'Docker', 'complement', 'Azure поддерживает Docker'),
            
            # Альтернативы
            ('React', 'Vue', 'alternative', 'React и Vue - альтернативные фреймворки'),
            ('AWS', 'Azure', 'alternative', 'AWS и Azure - альтернативные облачные платформы'),
        ]
        
        created_count = 0
        for tech1_name, tech2_name, relationship_type, description in relationships:
            # Получаем ID компетенций
            cur.execute("SELECT id FROM competencies WHERE name = %s", (tech1_name,))
            tech1_result = cur.fetchone()
            
            cur.execute("SELECT id FROM competencies WHERE name = %s", (tech2_name,))
            tech2_result = cur.fetchone()
            
            if tech1_result and tech2_result:
                tech1_id = tech1_result[0]
                tech2_id = tech2_result[0]
                
                # Создаем связь (односторонняя для избежания дублирования)
                cur.execute("""
                    INSERT INTO competency_relationships 
                    (competency_a_id, competency_b_id, relationship_type, description) 
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (tech1_id, tech2_id, relationship_type, description))
                
                created_count += 1
                print(f"  ✅ {tech1_name} → {tech2_name} ({relationship_type})")
            else:
                print(f"  ⚠️ Не найдены компетенции: {tech1_name} или {tech2_name}")
        
        conn.commit()
        print(f"✅ Создано {created_count} связей между технологиями")
        
    except Exception as e:
        print(f"❌ Ошибка создания связей: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def create_frequency_based_relationships():
    """Создаем связи на основе частоты упоминаний (популярные технологии связаны)"""
    conn = get_connection()
    cur = conn.cursor()
    
    print("\n🔗 Создаем связи на основе популярности...")
    
    try:
        # Получаем топ технологии (частота > 30)
        cur.execute("""
            SELECT c.id, c.name, vt.frequency 
            FROM competencies c 
            JOIN vacancy_technologies vt ON c.id = vt.competency_id 
            WHERE vt.frequency > 30 
            ORDER BY vt.frequency DESC
        """)
        
        top_technologies = cur.fetchall()
        created_count = 0
        
        print(f"  Найдено {len(top_technologies)} популярных технологий")
        
        # Создаем связи между топ технологиями
        for i, (tech1_id, tech1_name, freq1) in enumerate(top_technologies):
            for tech2_id, tech2_name, freq2 in top_technologies[i+1:]:
                if tech1_id != tech2_id:
                    # Вычисляем силу связи на основе частоты
                    strength = min(freq1, freq2) / max(freq1, freq2)
                    
                    cur.execute("""
                        INSERT INTO competency_relationships 
                        (competency_a_id, competency_b_id, relationship_type, description, strength) 
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (
                        tech1_id, 
                        tech2_id, 
                        'popular_together',
                        f'Популярные технологии ({freq1} и {freq2} упоминаний)',
                        strength
                    ))
                    created_count += 1
        
        conn.commit()
        print(f"✅ Создано {created_count} связей на основе популярности")
        
    except Exception as e:
        print(f"❌ Ошибка создания связей по популярности: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def create_category_relationships():
    """Создаем связи внутри категорий"""
    conn = get_connection()
    cur = conn.cursor()
    
    print("\n🔗 Создаем связи внутри категорий...")
    
    try:
        # Получаем компетенции по категориям
        cur.execute("""
            SELECT category, array_agg(id), array_agg(name)
            FROM competencies 
            GROUP BY category 
            HAVING COUNT(*) > 1
        """)
        
        categories = cur.fetchall()
        total_created = 0
        
        for category, ids, names in categories:
            created_count = 0
            # Создаем связи внутри категории (только для небольших категорий)
            if len(ids) <= 10:  # Избегаем слишком много связей
                for i, id1 in enumerate(ids):
                    for id2 in ids[i+1:]:
                        if id1 != id2:
                            cur.execute("""
                                INSERT INTO competency_relationships 
                                (competency_a_id, competency_b_id, relationship_type, description) 
                                VALUES (%s, %s, %s, %s)
                                ON CONFLICT DO NOTHING
                            """, (
                                id1, 
                                id2, 
                                'same_category',
                                f'Обе относятся к категории: {category}'
                            ))
                            created_count += 1
            
            total_created += created_count
            print(f"  • {category}: {created_count} связей")
        
        conn.commit()
        print(f"✅ Создано {total_created} связей внутри категорий")
        
    except Exception as e:
        print(f"❌ Ошибка создания связей по категориям: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def show_relationships_summary():
    """Показываем статистику по созданным связям"""
    conn = get_connection()
    cur = conn.cursor()
    
    print("\n📊 СТАТИСТИКА СВЯЗЕЙ:")
    print("=" * 50)
    
    # Общее количество связей
    cur.execute("SELECT COUNT(*) FROM competency_relationships")
    total_count = cur.fetchone()[0]
    print(f"📈 Всего связей: {total_count}")
    
    # По типам связей
    cur.execute("""
        SELECT relationship_type, COUNT(*) 
        FROM competency_relationships 
        GROUP BY relationship_type 
        ORDER BY COUNT(*) DESC
    """)
    
    print("\n🔗 По типам связей:")
    for rel_type, count in cur.fetchall():
        print(f"  • {rel_type}: {count} связей")
    
    # Топ компетенций по количеству связей
    cur.execute("""
        SELECT c.name, COUNT(*) as connections
        FROM competency_relationships cr
        JOIN competencies c ON c.id = cr.competency_a_id
        GROUP BY c.id, c.name
        ORDER BY connections DESC
        LIMIT 10
    """)
    
    print("\n🏆 Топ-10 компетенций по связям:")
    for name, connections in cur.fetchall():
        print(f"  • {name}: {connections} связей")
    
    # Примеры связей
    cur.execute("""
        SELECT 
            ca.name as comp_a,
            cb.name as comp_b,
            cr.relationship_type,
            cr.description
        FROM competency_relationships cr
        JOIN competencies ca ON ca.id = cr.competency_a_id
        JOIN competencies cb ON cb.id = cr.competency_b_id
        ORDER BY cr.created_at
        LIMIT 10
    """)
    
    print("\n💡 Примеры связей:")
    for comp_a, comp_b, rel_type, desc in cur.fetchall():
        print(f"  • {comp_a} → {comp_b} ({rel_type})")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    print("🚀 Создание связей между компетенциями...")
    
    try:
        # 1. Создаем таблицу если её нет
        create_relationships_table()
        
        # 2. Создаем связи разных типов
        create_technology_relationships()
        create_frequency_based_relationships() 
        create_category_relationships()
        
        # 3. Показываем статистику
        show_relationships_summary()
        
        print("\n✅ Связи между компетенциями созданы!")
        print("📊 Проверьте результат: python3 check_data.py")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()