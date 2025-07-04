import psycopg2
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
        return None

def show_current_tables():
    """Показать текущие таблицы в БД"""
    conn = get_connection()
    if not conn:
        return []
    
    try:
        cur = conn.cursor()
        
        # Получаем таблицы
        cur.execute("""
            SELECT table_name, 
                   (SELECT COUNT(*) FROM information_schema.columns 
                    WHERE table_name = t.table_name) as column_count
            FROM information_schema.tables t
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)
        tables = cur.fetchall()
        
        # Получаем представления
        cur.execute("""
            SELECT table_name
            FROM information_schema.views 
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)
        views = [row[0] for row in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        return tables, views
        
    except Exception as e:
        print(f"❌ Ошибка получения списка объектов: {e}")
        return [], []

def clean_database_soft():
    """Мягкая очистка - удаление только таблиц данных"""
    conn = get_connection()
    if not conn:
        return False
    
    try:
        cur = conn.cursor()
        
        print("🧹 МЯГКАЯ ОЧИСТКА БД")
        print("-" * 40)
        
        # Удаляем представления
        views_to_drop = [
            'olap_competency_analysis',
            'tech_market_summary', 
            'role_tech_salary_cube'
        ]
        
        print("📊 Удаление представлений:")
        for view in views_to_drop:
            try:
                cur.execute(f"DROP VIEW IF EXISTS {view} CASCADE")
                print(f"  ✅ {view}")
            except Exception as e:
                print(f"  ⚠️ {view}: {e}")
        
        # Удаляем таблицы данных
        tables_to_drop = [
            'vacancy_technologies_detailed',  # Сначала дочерние
            'vacancy_details',
            'fgos_competencies',
            'otf_td_standards'
        ]
        
        print("\n🗄️ Удаление таблиц:")
        for table in tables_to_drop:
            try:
                cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
                print(f"  ✅ {table}")
            except Exception as e:
                print(f"  ⚠️ {table}: {e}")
        
        conn.commit()
        print("\n✅ Мягкая очистка завершена")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка мягкой очистки: {e}")
        conn.rollback()
        return False
    finally:
        cur.close()
        conn.close()

def clean_database_hard():
    """Жесткая очистка - полное пересоздание схемы"""
    conn = get_connection()
    if not conn:
        return False
    
    try:
        cur = conn.cursor()
        
        print("💥 ЖЕСТКАЯ ОЧИСТКА БД")
        print("-" * 40)
        
        # Удаляем всю схему public
        cur.execute("DROP SCHEMA public CASCADE")
        print("  ✅ Схема public удалена")
        
        # Создаем заново
        cur.execute("CREATE SCHEMA public")
        print("  ✅ Схема public создана")
        
        # Выдаем права
        cur.execute("GRANT ALL ON SCHEMA public TO practice_user")
        cur.execute("GRANT ALL ON SCHEMA public TO public")
        print("  ✅ Права выданы")
        
        conn.commit()
        print("\n✅ Жесткая очистка завершена")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка жесткой очистки: {e}")
        conn.rollback()
        return False
    finally:
        cur.close()
        conn.close()

def show_database_status():
    """Показать текущий статус БД"""
    print(f"\n📊 ТЕКУЩИЙ СТАТУС БД")
    print("=" * 50)
    
    tables, views = show_current_tables()
    
    if not tables and not views:
        print("✅ База данных пуста")
        return
    
    if tables:
        print(f"🗄️ ТАБЛИЦЫ ({len(tables)}):")
        for table_name, col_count in tables:
            # Получаем количество записей
            conn = get_connection()
            if conn:
                try:
                    cur = conn.cursor()
                    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
                    row_count = cur.fetchone()[0]
                    print(f"  📊 {table_name}: {row_count:,} записей ({col_count} столбцов)")
                    cur.close()
                    conn.close()
                except:
                    print(f"  📊 {table_name}: ошибка подсчета записей")
    
    if views:
        print(f"\n📈 ПРЕДСТАВЛЕНИЯ ({len(views)}):")
        for view in views:
            print(f"  📊 {view}")

def main():
    """Главная функция меню очистки"""
    print("🗑️ УПРАВЛЕНИЕ ОЧИСТКОЙ БАЗЫ ДАННЫХ")
    print(f"⏰ Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # Показываем текущий статус
    show_database_status()
    
    if not get_connection():
        print("❌ Нет подключения к БД. Проверьте docker-compose up -d")
        return
    
    print(f"\n🎯 ВАРИАНТЫ ОЧИСТКИ:")
    print("1. 🧹 Мягкая очистка (удалить только таблицы данных)")
    print("2. 💥 Жесткая очистка (полное пересоздание схемы)")
    print("3. 📊 Показать статус БД")
    print("4. ❌ Отмена")
    
    try:
        choice = input("\nВыберите действие (1-4): ").strip()
        
        if choice == '1':
            print(f"\n⚠️ ВНИМАНИЕ: Будут удалены все таблицы и представления!")
            confirm = input("Продолжить? (y/N): ").strip().lower()
            
            if confirm in ['y', 'yes', 'да']:
                if clean_database_soft():
                    print(f"\n🎯 Следующий шаг: python3 db_loader.py")
            else:
                print("❌ Отменено")
        
        elif choice == '2':
            print(f"\n⚠️ КРИТИЧЕСКОЕ ВНИМАНИЕ: Будет полностью пересоздана схема БД!")
            print("Это удалит ВСЕ объекты в базе данных!")
            confirm = input("Вы АБСОЛЮТНО уверены? (y/N): ").strip().lower()
            
            if confirm in ['y', 'yes', 'да']:
                double_confirm = input("Последнее предупреждение! Продолжить? (y/N): ").strip().lower()
                if double_confirm in ['y', 'yes', 'да']:
                    if clean_database_hard():
                        print(f"\n🎯 Следующий шаг: python3 db_loader.py")
                else:
                    print("❌ Отменено")
            else:
                print("❌ Отменено")
        
        elif choice == '3':
            show_database_status()
        
        elif choice == '4':
            print("❌ Отменено пользователем")
        
        else:
            print("❌ Неверный выбор")
            
    except KeyboardInterrupt:
        print("\n❌ Прервано пользователем")
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    main()