import json
from langchain_core.messages import SystemMessage
from langchain_gigachat.chat_models import GigaChat

# Конфигурация
CONFIG = {
    "input_json": r"combined_output.json",
    "output_json": r"pauses_results.json",
    "operator_id": 1,
    "call_id": 1
}

# Инициализация GigaChat
giga = GigaChat(
    credentials="YTUyNTBhODAtYThjYi00OTU0LWJmZTEtNjJkYWE5NGE0NmExOjY3MDI2NjExLTdhOTktNGVkZS04ZTMxLWY0Njk2NWUxM2YyZQ==",
    verify_ssl_certs=False,
)

def load_call_from_json(json_path: str) -> str:
    """Загрузка диалога из JSON-файла"""
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return "\n".join(
            f"{entry.get('speaker', '').upper()}: {entry.get('text', '').strip()}"
            for entry in data
            if entry.get("speaker", "").upper() not in ["SPEAKER_UNKNOWN", ""]
        )
    except Exception as e:
        print(f"Ошибка загрузки файла: {e}")
        return ""

def determine_operator_speaker(raw_text: str) -> str:
    """Определение оператора через GigaChat"""
    prompt = (
        "Определи, какой спикер (SPEAKER_00 или SPEAKER_01) является оператором VinPin. "
        "Ответь только идентификатором спикера.\n\nРазговор:\n" + raw_text
    )
    try:
        response = giga.invoke([SystemMessage(content=prompt)]).content.strip()
        return response if response in ["SPEAKER_00", "SPEAKER_01"] else "SPEAKER_00"
    except Exception as e:
        print(f"Ошибка определения оператора: {e}")
        return "SPEAKER_00"

def calculate_pauses(json_path: str, operator_speaker: str) -> float:
    """Расчет суммарных пауз оператора"""
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return sum(
            float(entry.get("pause_before", 0))
            for entry in data
            if entry.get("speaker", "").upper() == operator_speaker
        )
    except Exception as e:
        print(f"Ошибка расчета пауз: {e}")
        return 0.0

def save_pauses_results(total_pause: float, operator: str):
    """Сохранение результатов пауз в отдельный JSON"""
    result = {
        "call_id": CONFIG["call_id"],
        "operator_id": CONFIG["operator_id"],
        "total_pause_seconds": round(total_pause, 2)
    }
    
    try:
        with open(CONFIG["output_json"], 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        print(f"Результаты пауз сохранены в {CONFIG['output_json']}")
    except Exception as e:
        print(f"Ошибка сохранения: {e}")

if __name__ == "__main__":
    # Определение оператора
    raw_text = load_call_from_json(CONFIG["input_json"])
    operator = determine_operator_speaker(raw_text)
    
    # Расчет пауз
    total_pause = calculate_pauses(CONFIG["input_json"], operator)
    
    # Сохранение результатов
    save_pauses_results(total_pause, operator)
    
    print(f"Суммарные паузы оператора {operator}: {total_pause:.2f} сек.")