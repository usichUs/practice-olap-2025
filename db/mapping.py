import pandas as pd
import json

def create_fgos_mapping():
    """Создание маппинга технологий к компетенциям ФГОС"""
    
    # Читаем данные ФГОС
    fgos_df = pd.read_csv('csv_files/fgos_competencies.csv')
    
    # Маппинг технологий к компетенциям ФГОС
    tech_fgos_mapping = {
        # Языки программирования -> ПК-1 (Способен разрабатывать программное обеспечение)
        'Python': ['ПК-1.1', 'ПК-1.2', 'ПК-2.1'],
        'JavaScript': ['ПК-1.1', 'ПК-1.2'],
        'Java': ['ПК-1.1', 'ПК-1.2', 'ПК-2.1'],
        'TypeScript': ['ПК-1.1', 'ПК-1.2'],
        
        # Фреймворки -> ПК-2 (Способен применять современные технологии)
        'React': ['ПК-1.2', 'ПК-2.1'],
        'Vue': ['ПК-1.2', 'ПК-2.1'],
        'Django': ['ПК-1.2', 'ПК-2.1'],
        'Flask': ['ПК-1.2', 'ПК-2.1'],
        
        # Базы данных -> ПК-3 (Способен проектировать информационные системы)
        'SQL': ['ПК-2.1', 'ПК-3.1'],
        'PostgreSQL': ['ПК-2.1', 'ПК-3.1'],
        'MongoDB': ['ПК-2.1', 'ПК-3.1'],
        
        # DevOps -> ПК-4 (Способен сопровождать программное обеспечение)
        'Docker': ['ПК-2.2', 'ПК-4.1'],
        'Kubernetes': ['ПК-2.2', 'ПК-4.1'],
        'CI/CD': ['ПК-2.2', 'ПК-4.1'],
        
        # Инструменты -> УК (Универсальные компетенции)
        'Git': ['УК-1.1', 'ПК-1.1'],
        'Linux': ['ПК-2.2', 'ПК-4.1']
    }
    
    return tech_fgos_mapping

def create_prof_standards_mapping():
    """Создание маппинга технологий к профстандартам"""
    
    # Маппинг технологий к ОТФ профстандартов
    tech_prof_mapping = {
        # 06.001 Программист
        'Python': ['06.001_A.1', '06.001_A.2', '06.001_B.1'],
        'JavaScript': ['06.001_A.1', '06.001_A.2'],
        'Java': ['06.001_A.1', '06.001_A.2', '06.001_B.1'],
        'React': ['06.001_A.2'],
        'Vue': ['06.001_A.2'],
        'Django': ['06.001_A.2', '06.001_B.1'],
        'SQL': ['06.001_A.3', '06.001_B.2'],
        'Git': ['06.001_A.1'],
        'Docker': ['06.001_B.3'],
        
        # 06.022 Системный аналитик
        'SQL': ['06.022_A.1', '06.022_A.2'],
        'Python': ['06.022_A.3', '06.022_B.1']
    }
    
    return tech_prof_mapping

def save_mappings():
    """Сохранение маппингов в файлы"""
    
    fgos_mapping = create_fgos_mapping()
    prof_mapping = create_prof_standards_mapping()
    
    # Сохраняем в JSON для удобства
    with open('mapped_data/tech_fgos_mapping.json', 'w', encoding='utf-8') as f:
        json.dump(fgos_mapping, f, ensure_ascii=False, indent=2)
    
    with open('mapped_data/tech_prof_mapping.json', 'w', encoding='utf-8') as f:
        json.dump(prof_mapping, f, ensure_ascii=False, indent=2)
    
    # Также создаем CSV для ClickHouse
    mapping_data = []
    
    for tech, competencies in fgos_mapping.items():
        for comp in competencies:
            mapping_data.append({
                'technology': tech,
                'competency_code': comp,
                'competency_type': 'FGOS',
                'standard_code': '09.03.03' if comp.startswith('ПК') else 'UK'
            })
    
    for tech, standards in prof_mapping.items():
        for standard in standards:
            standard_code = standard.split('_')[0]
            mapping_data.append({
                'technology': tech,
                'competency_code': standard,
                'competency_type': 'PROF_STANDARD',
                'standard_code': standard_code
            })
    
    pd.DataFrame(mapping_data).to_csv('mapped_data/07_tech_competency_mapping.csv', 
                                     index=False, encoding='utf-8')
    
    print("✅ Маппинги созданы:")
    print("  📄 tech_fgos_mapping.json")
    print("  📄 tech_prof_mapping.json") 
    print("  📄 07_tech_competency_mapping.csv")

if __name__ == "__main__":
    save_mappings()