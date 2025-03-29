import json

# Конфигурация путей
CONFIG = {
    "key_results": r"analysis_results.json",
    "pauses_results": r"pauses_results.json"
}

# База знаний с рекомендациями
RECOMMENDATIONS = {
    "penalty_levels": {
        "low": (0.0, 0.3),
        "medium": (0.3, 0.7),
        "high": (0.7, 1.0)
    },
    
    "pause_thresholds": {
        "short": 30.0,
        "long": 60.0
    },
    
    "advice_templates": {
        "low_penalty": [
            "Отличная работа! Продолжайте в том же духе.",
            "Ваши показатели лучше среднего по команде."
        ],
        "medium_penalty": [
            "Обратите внимание на использование ключевых фраз.",
            "Практикуйтесь в выявлении потребностей клиентов."
        ],
        "high_penalty": [
            "Требуется дополнительное обучение.",
            "Анализируйте свои разговоры с наставником."
        ],
        "pauses_short": [
            "Оптимальное время обработки реплик."
        ],
        "pauses_medium": [
            "Следите за длительными паузами в диалоге.",
            "Используйте паузы для анализа речи клиента"
        ],
        "pauses_long": [
            "Слишком длинные паузы раздражают клиентов.",
            "Тренируйтесь быстрее формулировать ответы"
        ]
    }
}

def load_data(file_path: str) -> dict:
    """Загрузка данных из JSON-файла"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Ошибка загрузки {file_path}: {e}")
        return {}

def get_penalty_level(penalty: float) -> str:
    """Определение уровня штрафных баллов"""
    if penalty < RECOMMENDATIONS["penalty_levels"]["low"][1]:
        return "low"
    elif penalty < RECOMMENDATIONS["penalty_levels"]["medium"][1]:
        return "medium"
    else:
        return "high"

def get_pause_level(pause_seconds: float) -> str:
    """Определение уровня пауз"""
    if pause_seconds < RECOMMENDATIONS["pause_thresholds"]["short"]:
        return "short"
    elif pause_seconds < RECOMMENDATIONS["pause_thresholds"]["long"]:
        return "medium"
    else:
        return "long"

def generate_recommendations() -> dict:
    """Генерация персонализированных рекомендаций"""
    # Загрузка данных
    key_data = load_data(CONFIG["key_results"])
    pauses_data = load_data(CONFIG["pauses_results"])
    
    # Проверка наличия данных
    if not key_data or not pauses_data:
        return {"error": "Недостаточно данных для анализа"}
    
    # Анализ показателей
    penalty = key_data.get("penalty", 0.0)
    pauses = pauses_data.get("total_pause_seconds", 0.0)
    
    # Определение уровней
    penalty_level = get_penalty_level(penalty)
    pause_level = get_pause_level(pauses)
    
    # Формирование рекомендаций
    recommendations = []
    
    # Рекомендации по ключевым показателям
    recommendations.extend(
        RECOMMENDATIONS["advice_templates"][f"{penalty_level}_penalty"]
    )
    
    # Рекомендации по паузам
    recommendations.extend(
        RECOMMENDATIONS["advice_templates"][f"pauses_{pause_level}"]
    )
    
    # Дополнительные комбинированные рекомендации
    if penalty_level == "high" and pause_level == "long":
        recommendations.append("Рекомендуем пройти курс 'Эффективные коммуникации'")
    
    if penalty_level == "medium" and pause_level == "short":
        recommendations.append("Попробуйте больше использовать активное слушание")
    
    return {
        "call_id": key_data.get("call_id"),
        "operator_id": key_data.get("operator_id"),
        "recommendations": list(set(recommendations))  # Удаление дубликатов
    }

def save_recommendations(recommendations: dict):
    """Сохранение рекомендаций в JSON"""
    output_path = r"C:\Users\User\Desktop\analization\analization_2\final_report.json"
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(recommendations, f, indent=2, ensure_ascii=False)
        print(f"Рекомендации сохранены в {output_path}")
    except Exception as e:
        print(f"Ошибка сохранения: {e}")

if __name__ == "__main__":
    report = generate_recommendations()
    if "error" not in report:
        save_recommendations(report)
        print("\nРекомендации для оператора:")
        for i, advice in enumerate(report["recommendations"], 1):
            print(f"{i}. {advice}")
    else:
        print(report["error"])