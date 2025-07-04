import requests
import re
import time
import csv
import pandas as pd
from collections import defaultdict, Counter
from datetime import datetime

class HHEnhancedParser:
    def __init__(self):
        self.base_url = "https://api.hh.ru"
        self.headers = {'User-Agent': 'HH-User-Agent'}
        
        # Расширенная карта технологий с привязкой к компетенциям ФГОС/Профстандартов
        self.tech_competency_mapping = {
            # Программирование
            'Python': {
                'category': 'Язык программирования',
                'fgos_competencies': ['ПК-1', 'ПК-2', 'ПК-3'],
                'prof_standards': ['06.001_A', '06.001_B'],
                'level': 'middle',
                'domain': 'backend'
            },
            'JavaScript': {
                'category': 'Язык программирования', 
                'fgos_competencies': ['ПК-1', 'ПК-2'],
                'prof_standards': ['06.001_A'],
                'level': 'middle',
                'domain': 'frontend'
            },
            'React': {
                'category': 'Фреймворк',
                'fgos_competencies': ['ПК-1', 'ПК-2'],
                'prof_standards': ['06.001_A'],
                'level': 'middle',
                'domain': 'frontend'
            },
            'SQL': {
                'category': 'База данных',
                'fgos_competencies': ['ПК-2', 'ПК-3'],
                'prof_standards': ['06.001_B', '06.022_A'],
                'level': 'basic',
                'domain': 'data'
            },
            'Docker': {
                'category': 'DevOps',
                'fgos_competencies': ['ПК-3', 'ПК-4'],
                'prof_standards': ['06.001_C'],
                'level': 'advanced',
                'domain': 'infrastructure'
            },
            'Git': {
                'category': 'Инструмент',
                'fgos_competencies': ['ПК-1'],
                'prof_standards': ['06.001_A'],
                'level': 'basic',
                'domain': 'development'
            }
        }
        
        # Ключевые слова для определения ролей
        self.role_keywords = {
            'backend': ['backend', 'бэкенд', 'серверная', 'api', 'микросервис'],
            'frontend': ['frontend', 'фронтенд', 'ui', 'ux', 'интерфейс'],
            'fullstack': ['fullstack', 'фулстек', 'полный цикл'],
            'data': ['data', 'данные', 'аналитик', 'bi', 'etl'],
            'devops': ['devops', 'деопс', 'инфраструктура', 'администратор'],
            'mobile': ['mobile', 'мобильн', 'android', 'ios', 'flutter'],
            'qa': ['qa', 'тестировщик', 'тестирование', 'автотест']
        }
        
        # Уровни опыта
        self.experience_mapping = {
            'Нет опыта': 'junior',
            'От 1 года до 3 лет': 'junior',
            'От 3 до 6 лет': 'middle', 
            'Более 6 лет': 'senior'
        }

    def search_vacancies_enhanced(self, queries, max_pages=3):
        """Расширенный поиск вакансий с детальной информацией"""
        all_vacancies = []
        
        for query in queries:
            print(f"🔍 Поиск по запросу: {query}")
            
            for page in range(max_pages):
                params = {
                    'text': query,
                    'page': page,
                    'per_page': 100,
                    'area': 1,  # Москва
                    'only_with_salary': 'false'
                }
                
                try:
                    response = requests.get(f"{self.base_url}/vacancies", 
                                          params=params, headers=self.headers)
                    data = response.json()
                    
                    vacancies = data.get('items', [])
                    if not vacancies:
                        break
                        
                    # Получаем детальную информацию сразу
                    detailed_vacancies = self.get_detailed_vacancies(vacancies)
                    all_vacancies.extend(detailed_vacancies)
                    
                    print(f"  Страница {page + 1}: {len(detailed_vacancies)} вакансий")
                    time.sleep(0.5)
                    
                except Exception as e:
                    print(f"Ошибка: {e}")
                    break
        
        return all_vacancies

    def get_detailed_vacancies(self, vacancy_list):
        """Получение детальной информации по вакансиям"""
        detailed = []
        
        for vacancy in vacancy_list:
            try:
                # Базовая информация
                vacancy_id = vacancy['id']
                
                # Получаем полное описание
                response = requests.get(f"{self.base_url}/vacancies/{vacancy_id}",
                                      headers=self.headers)
                full_data = response.json()
                
                processed_vacancy = self.process_vacancy_data(full_data)
                detailed.append(processed_vacancy)
                
                time.sleep(0.2)  # Небольшая задержка
                
            except Exception as e:
                print(f"Ошибка обработки вакансии {vacancy.get('id')}: {e}")
                continue
        
        return detailed

    def process_vacancy_data(self, vacancy_data):
        """Обработка данных вакансии с извлечением всей нужной информации"""
        
        # Базовые данные
        processed = {
            'vacancy_id': vacancy_data.get('id'),
            'title': vacancy_data.get('name'),
            'company': vacancy_data.get('employer', {}).get('name'),
            'company_size': self.get_company_size(vacancy_data.get('employer', {})),
            'area': vacancy_data.get('area', {}).get('name'),
            'published_date': vacancy_data.get('published_at'),
            'experience_raw': vacancy_data.get('experience', {}).get('name'),
            'experience_level': self.map_experience_level(vacancy_data.get('experience', {}).get('name')),
            'schedule': vacancy_data.get('schedule', {}).get('name'),
            'employment': vacancy_data.get('employment', {}).get('name'),
        }
        
        # Зарплата
        salary = vacancy_data.get('salary')
        if salary:
            processed.update({
                'salary_from': salary.get('from'),
                'salary_to': salary.get('to'),
                'salary_currency': salary.get('currency'),
                'salary_gross': salary.get('gross'),
                'avg_salary': self.calculate_avg_salary(salary)
            })
        
        # Полный текст для анализа
        description = vacancy_data.get('description', '') or ''
        key_skills = vacancy_data.get('key_skills', [])
        skills_text = ' '.join([skill.get('name', '') for skill in key_skills])
        
        full_text = f"{processed['title']} {description} {skills_text}"
        
        # Извлечение технологий с детальной информацией
        technologies = self.extract_technologies_detailed(full_text)
        processed['technologies'] = technologies
        processed['tech_count'] = len(technologies)
        
        # Определение роли/домена
        processed['role'] = self.determine_role(full_text)
        processed['domain'] = self.determine_domain(full_text)
        
        # Ключевые навыки
        processed['key_skills'] = [skill.get('name') for skill in key_skills]
        processed['skills_count'] = len(key_skills)
        
        # Связь с компетенциями ФГОС/Профстандартов
        competencies = self.map_to_competencies(technologies)
        processed['fgos_competencies'] = competencies['fgos']
        processed['prof_standard_competencies'] = competencies['prof_standards']
        
        return processed

    def extract_technologies_detailed(self, text):
        """Детальное извлечение технологий с метаинформацией"""
        if not text:
            return {}
        
        text_lower = text.lower()
        found_technologies = {}
        
        for tech, info in self.tech_competency_mapping.items():
            pattern = r'\b' + re.escape(tech.lower()) + r'\b'
            matches = len(re.findall(pattern, text_lower))
            
            if matches > 0:
                found_technologies[tech] = {
                    'frequency': matches,
                    'category': info['category'],
                    'level': info['level'],
                    'domain': info['domain'],
                    'fgos_competencies': info['fgos_competencies'],
                    'prof_standards': info['prof_standards']
                }
        
        return found_technologies

    def map_to_competencies(self, technologies):
        """Сопоставление технологий с компетенциями"""
        fgos_competencies = set()
        prof_standards = set()
        
        for tech, info in technologies.items():
            fgos_competencies.update(info.get('fgos_competencies', []))
            prof_standards.update(info.get('prof_standards', []))
        
        return {
            'fgos': list(fgos_competencies),
            'prof_standards': list(prof_standards)
        }

    def determine_role(self, text):
        """Определение роли разработчика"""
        text_lower = text.lower()
        role_scores = defaultdict(int)
        
        for role, keywords in self.role_keywords.items():
            for keyword in keywords:
                role_scores[role] += len(re.findall(r'\b' + re.escape(keyword) + r'\b', text_lower))
        
        return max(role_scores, key=role_scores.get) if role_scores else 'general'

    def determine_domain(self, text):
        """Определение предметной области"""
        domain_keywords = {
            'fintech': ['банк', 'финанс', 'платеж', 'криптовалют'],
            'ecommerce': ['интернет-магазин', 'ecommerce', 'торговл'],
            'gamedev': ['игр', 'геймдев', 'unity', 'unreal'],
            'edtech': ['образование', 'обучение', 'курс'],
            'healthtech': ['медицин', 'здоровье', 'клиник'],
            'government': ['государств', 'госуслуг', 'бюджет']
        }
        
        text_lower = text.lower()
        domain_scores = defaultdict(int)
        
        for domain, keywords in domain_keywords.items():
            for keyword in keywords:
                domain_scores[domain] += len(re.findall(r'\b' + re.escape(keyword) + r'\b', text_lower))
        
        return max(domain_scores, key=domain_scores.get) if domain_scores else 'general'

    def get_company_size(self, employer_data):
        """Определение размера компании"""
        if not employer_data:
            return None
        
        # Можно расширить логику определения размера
        name = employer_data.get('name', '').lower()
        if any(word in name for word in ['яндекс', 'сбер', 'mail', 'vk']):
            return 'large'
        return 'unknown'

    def map_experience_level(self, experience_raw):
        """Маппинг уровня опыта"""
        return self.experience_mapping.get(experience_raw, 'unknown')

    def calculate_avg_salary(self, salary_data):
        """Расчет средней зарплаты"""
        if not salary_data:
            return None
        
        salary_from = salary_data.get('from')
        salary_to = salary_data.get('to')
        
        if salary_from and salary_to:
            return (salary_from + salary_to) / 2
        return salary_from or salary_to

    def save_enhanced_csv(self, processed_vacancies, timestamp=None):
        """Сохранение расширенных данных в CSV"""
        if not timestamp:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 1. Основной файл с вакансиями
        vacancies_file = f'csv_for_export/hh_vacancies_enhanced_{timestamp}.csv'
        
        vacancies_data = []
        for vacancy in processed_vacancies:
            row = {
                'vacancy_id': vacancy['vacancy_id'],
                'title': vacancy['title'],
                'company': vacancy['company'],
                'company_size': vacancy.get('company_size'),
                'area': vacancy['area'],
                'published_date': vacancy['published_date'],
                'experience_raw': vacancy['experience_raw'],
                'experience_level': vacancy['experience_level'],
                'role': vacancy['role'],
                'domain': vacancy['domain'],
                'salary_from': vacancy.get('salary_from'),
                'salary_to': vacancy.get('salary_to'),
                'avg_salary': vacancy.get('avg_salary'),
                'tech_count': vacancy['tech_count'],
                'skills_count': vacancy['skills_count'],
                'fgos_competencies_count': len(vacancy['fgos_competencies']),
                'prof_competencies_count': len(vacancy['prof_standard_competencies'])
            }
            vacancies_data.append(row)
        
        pd.DataFrame(vacancies_data).to_csv(vacancies_file, index=False, encoding='utf-8')
        
        # 2. Детальный файл технологий
        tech_file = f'csv_for_export/hh_technologies_detailed_{timestamp}.csv'
        tech_data = []
        
        for vacancy in processed_vacancies:
            for tech, info in vacancy['technologies'].items():
                tech_data.append({
                    'vacancy_id': vacancy['vacancy_id'],
                    'technology': tech,
                    'frequency': info['frequency'],
                    'category': info['category'],
                    'level': info['level'],
                    'domain': info['domain'],
                    'fgos_competencies': ','.join(info['fgos_competencies']),
                    'prof_standards': ','.join(info['prof_standards'])
                })
        
        pd.DataFrame(tech_data).to_csv(tech_file, index=False, encoding='utf-8')
        
        # 3. Сводная аналитика
        analytics_file = f'csv_for_export/hh_analytics_{timestamp}.csv'
        self.create_analytics_summary(processed_vacancies, analytics_file)
        
        print(f"✅ Сохранены файлы:")
        print(f"  📄 Вакансии: {vacancies_file}")
        print(f"  🔧 Технологии: {tech_file}")
        print(f"  📊 Аналитика: {analytics_file}")

    def create_analytics_summary(self, processed_vacancies, filename):
        """Создание сводной аналитики"""
        analytics = []
        
        # Агрегация по технологиям
        tech_stats = defaultdict(lambda: {
            'total_mentions': 0,
            'vacancy_count': 0,
            'avg_salary': [],
            'roles': defaultdict(int),
            'experience_levels': defaultdict(int),
            'domains': defaultdict(int)
        })
        
        for vacancy in processed_vacancies:
            for tech, info in vacancy['technologies'].items():
                stats = tech_stats[tech]
                stats['total_mentions'] += info['frequency']
                stats['vacancy_count'] += 1
                
                if vacancy.get('avg_salary'):
                    stats['avg_salary'].append(vacancy['avg_salary'])
                
                stats['roles'][vacancy['role']] += 1
                stats['experience_levels'][vacancy['experience_level']] += 1
                stats['domains'][vacancy['domain']] += 1
        
        # Формирование итоговой аналитики
        for tech, stats in tech_stats.items():
            analytics.append({
                'technology': tech,
                'total_mentions': stats['total_mentions'],
                'vacancy_count': stats['vacancy_count'],
                'avg_salary': sum(stats['avg_salary']) / len(stats['avg_salary']) if stats['avg_salary'] else None,
                'top_role': max(stats['roles'], key=stats['roles'].get) if stats['roles'] else None,
                'top_experience': max(stats['experience_levels'], key=stats['experience_levels'].get) if stats['experience_levels'] else None,
                'top_domain': max(stats['domains'], key=stats['domains'].get) if stats['domains'] else None
            })
        
        pd.DataFrame(analytics).to_csv(filename, index=False, encoding='utf-8')

    def run_enhanced_parsing(self):
        """Запуск расширенного парсинга"""
        print("🚀 Запуск расширенного парсинга HH")
        
        # Поисковые запросы для комплексного анализа
        queries = [
            "python разработчик",
            "backend разработчик", 
            "frontend разработчик",
            "fullstack разработчик",
            "системный аналитик",
            "data analyst",
            "devops инженер"
        ]
        
        # Сбор данных
        all_vacancies = self.search_vacancies_enhanced(queries, max_pages=2)
        
        print(f"\n📊 Собрано {len(all_vacancies)} вакансий")
        
        # Сохранение
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.save_enhanced_csv(all_vacancies, timestamp)
        
        return all_vacancies

if __name__ == "__main__":
    parser = HHEnhancedParser()
    vacancies = parser.run_enhanced_parsing()