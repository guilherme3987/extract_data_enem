import json
import csv
import time
from config import PUBLIC_PATH, BASE_PATH, OUTPUT_PATH, OUTPUT_JSON, OUTPUT_CSV
from logging_utils import log_extraction_stats, logger


def _parse_alternatives(alternatives: list) -> dict:
    """Retorna dicionário letra→texto e a letra correta."""
    parsed = {}
    correct = None
    for alt in alternatives:
        letter = alt.get("letter", "")
        parsed[letter] = alt.get("text", "")
        if alt.get("isCorrect"):
            correct = letter
    return parsed, correct


def extract_all_questions(**kwargs):
    start_time = time.time()

    # Validações de ambiente
    if not BASE_PATH.exists():
        logger.error(f"❌ BASE_PATH não encontrado: {BASE_PATH}")
        raise FileNotFoundError(f"Pasta {BASE_PATH} ausente.")

    if not PUBLIC_PATH.exists():
        logger.error(f"❌ PUBLIC_PATH não encontrado: {PUBLIC_PATH}")
        raise FileNotFoundError(f"Caminho {PUBLIC_PATH} não encontrado.")

    # Cria pasta de saída
    OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

    # Descobre todos os anos disponíveis
    anos = sorted(
        [d.name for d in PUBLIC_PATH.iterdir() if d.is_dir() and d.name.isdigit()]
    )
    logger.info(f"📅 Anos encontrados: {anos}")

    all_questions = []

    for ano in anos:
        questions_path = PUBLIC_PATH / ano / "questions"
        if not questions_path.exists():
            logger.warning(f"⚠️  Pasta questions/ ausente para o ano {ano}. Pulando.")
            continue

        # Cada subpasta é uma questão
        question_dirs = sorted(questions_path.iterdir(), key=lambda p: p.name)
        ano_count = 0

        for q_dir in question_dirs:
            details_file = q_dir / "details.json"
            if not details_file.exists():
                logger.warning(f"⚠️  details.json ausente em {q_dir}. Pulando.")
                continue

            with open(details_file, encoding="utf-8") as f:
                data = json.load(f)

            alternatives_raw = data.get("alternatives", [])
            alternatives_dict, correct_letter = _parse_alternatives(alternatives_raw)

            question = {
                "year":                     data.get("year"),
                "index":                    data.get("index"),
                "language":                 data.get("language"),          # espanhol | ingles | null
                "discipline":               data.get("discipline"),
                "context":                  data.get("context"),
                "alternatives_intro":       data.get("alternativesIntroduction"),
                "alternative_A":            alternatives_dict.get("A"),
                "alternative_B":            alternatives_dict.get("B"),
                "alternative_C":            alternatives_dict.get("C"),
                "alternative_D":            alternatives_dict.get("D"),
                "alternative_E":            alternatives_dict.get("E"),
                "correct_alternative":      data.get("correctAlternative") or correct_letter,
                "has_images":               len(data.get("files", [])) > 0,
            }

            all_questions.append(question)
            ano_count += 1

        logger.info(f"   ✅ Ano {ano}: {ano_count} questões extraídas.")

    # ── Salva JSON ──────────────────────────────────────────────────────────
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(all_questions, f, ensure_ascii=False, indent=2)
    logger.info(f"💾 JSON salvo em: {OUTPUT_JSON}")

    # ── Salva CSV ───────────────────────────────────────────────────────────
    if all_questions:
        fieldnames = list(all_questions[0].keys())
        with open(OUTPUT_CSV, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_questions)
        logger.info(f"💾 CSV  salvo em: {OUTPUT_CSV}")

    duration = time.time() - start_time
    log_extraction_stats(anos, len(all_questions), duration)

    # Retorna o caminho dos arquivos para o XCom do Airflow
    return {
        "total_questions": len(all_questions),
        "output_json":     str(OUTPUT_JSON),
        "output_csv":      str(OUTPUT_CSV),
    }