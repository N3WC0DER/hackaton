import os
import re
import json
from docx import Document
from typing import List, Dict
from datetime import datetime
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_gigachat.chat_models import GigaChat

# Инициализация модели GigaChat
giga = GigaChat(
    credentials="YTUyNTBhODAtYThjYi00OTU0LWJmZTEtNjJkYWE5NGE0NmExOjY3MDI2NjExLTdhOTktNGVkZS04ZTMxLWY0Njk2NWUxM2YyZQ==",
    verify_ssl_certs=False,
)

# Конфигурация
CONFIG = {
    "checklist_path": r"check_list.docx",
    "input_json": r"combined_output.json",
    "output_json": r"analysis_results.json",
    "operator_id": 1,
    "call_id": 1
}

def parse_checklist(doc_path: str) -> List[Dict]:
    """Полный парсинг Word-документа с чек-листом"""
    doc = Document(doc_path)
    competencies = []
    current_competency = None
    current_indicator = None

    for para in doc.paragraphs:
        text = para.text.strip()
        if not text:
            continue

        # Обработка компетенций
        if "Компетенция:" in text:
            comp_name = text.split("Компетенция:")[1].strip()
            current_competency = {
                "name": comp_name,
                "stage": None,
                "indicators": []
            }
            competencies.append(current_competency)
            continue

        # Обработка этапа
        if "Этап продаж:" in text:
            if current_competency:
                current_competency["stage"] = text.split("Этап продаж:")[1].strip()
            continue

        # Обработка индикаторов
        if re.match(r"^\d+\.\s*Индикатор", text, re.IGNORECASE):
            parts = text.split(":", 1)
            indicator_title = parts[1].strip() if len(parts) > 1 else text
            current_indicator = {
                "title": indicator_title,
                "penalty": 0,
                "examples": [],
                "validation": "required",
                "needs_gemini": False,
                "min_count": None
            }
            if current_competency:
                current_competency["indicators"].append(current_indicator)
            continue

        # Обработка штрафного балла
        if "Штрафной балл:" in text:
            if current_indicator:
                try:
                    penalty_value = int(text.split("Штрафной балл:")[1].strip())
                    current_indicator["penalty"] = penalty_value
                except ValueError:
                    current_indicator["penalty"] = 0
            continue

        # Примеры вопросов
        if "- Примеры вопросов:" in text:
            parts = text.split("Примеры вопросов:")
            if len(parts) > 1 and current_indicator:
                examples_text = parts[1].strip()
                examples = [ex.strip() + "?" for ex in examples_text.split("?") if ex.strip()]
                current_indicator["examples"] = examples
            continue

        # Обработка условия "не менее"
        if "не менее" in text.lower():
            match = re.search(r'не менее (\d+)', text, re.IGNORECASE)
            if match and current_indicator:
                current_indicator["min_count"] = int(match.group(1))
            continue

        # Определение запрещенных действий
        if any(kw in text.lower() for kw in ["запрещено", "не должен", "избегать"]):
            if current_indicator:
                current_indicator["validation"] = "prohibited"
            continue

        # Определение сложных индикаторов для Gemini
        if any(kw in text.lower() for kw in ["активное слушание", "парафраз", "сторителлинг"]):
            if current_indicator:
                current_indicator["needs_gemini"] = True

    return competencies

def analyze_with_ai(text: str, indicator: Dict) -> bool:
    """Полный анализ через GigaChat"""
    messages = [
        SystemMessage(
            content=f"Анализ телефонного разговора.\nТребование: {indicator['title']}\nТекст разговора: {text}\nВыполнено ли требование? Ответ только ДА или НЕТ."
        )
    ]

    try:
        response = giga.invoke(messages).content
        return "ДА" in response.strip().upper()
    except Exception as e:
        print(f"Ошибка GigaChat: {e}")
        return False

def analyze_call(call_text: str, checklist: List[Dict]) -> dict:
    """Полная логика анализа звонка"""
    total_penalty = 0
    violations = []
    gemini_checks = 0

    all_indicators = [ind for comp in checklist for ind in comp.get("indicators", [])]
    
    # Разбиение на чанки для обработки
    chunk_size = 5
    for i in range(0, len(all_indicators), chunk_size):
        chunk = all_indicators[i:i + chunk_size]
        for indicator in chunk:
            gemini_checks += 1
            if not analyze_with_ai(call_text, indicator):
                total_penalty += indicator.get("penalty", 0)
                violations.append({
                    "indicator": indicator.get("title"),
                    "penalty": indicator.get("penalty", 0)
                })

    return {
        "total_penalty": total_penalty,
        "status": "GOOD" if total_penalty <= 50 else "BAD"
    }

def load_call_from_json(json_path: str) -> str:
    """Загрузка диалога с обработкой"""
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Ошибка загрузки JSON: {e}")
        return ""

    return "\n".join(
        f"{entry.get('speaker', '').upper()}: {entry.get('text', '').strip()}"
        for entry in data
        if entry.get("speaker", "").upper() not in ["SPEAKER_UNKNOWN", ""]
    )

def determine_roles_via_gigachat(original_call: str) -> str:
    """Определение ролей через GigaChat"""
    prompt = (
        "Определи роли спикеров в разговоре. Замени SPEAKER_00 на Оператор, SPEAKER_01 на Клиент. "
        "Верни только преобразованный текст:\n\n" + original_call
    )

    try:
        response = giga.invoke([SystemMessage(content=prompt)]).content
        return response.strip()
    except Exception as e:
        print(f"Ошибка определения ролей: {e}")
        return original_call

def save_results(result_data: dict):
    """Сохранение в JSON с требуемым форматом"""
    output = {
        "call_id": CONFIG["call_id"],
        "operator_id": CONFIG["operator_id"],
        "penalty": round(result_data["total_penalty"] / 100, 2),
        "status": result_data["status"]
    }

    try:
        with open(CONFIG["output_json"], 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        print(f"Результаты сохранены в {CONFIG['output_json']}")
    except Exception as e:
        print(f"Ошибка сохранения: {e}")

def main():
    """Основной рабочий процесс"""
    # Парсинг чек-листа
    checklist = parse_checklist(CONFIG["checklist_path"])
    
    # Загрузка и обработка звонка
    raw_call = load_call_from_json(CONFIG["input_json"])
    processed_call = determine_roles_via_gigachat(raw_call)
    
    # Анализ звонка
    analysis_result = analyze_call(processed_call, checklist)
    
    # Сохранение результатов
    save_results(analysis_result)

main()