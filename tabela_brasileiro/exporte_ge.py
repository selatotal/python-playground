import requests
import csv
from datetime import datetime

BASE_URL = "https://api.globoesporte.globo.com/tabela/009b5a68-dd09-46b8-95b3-293a2d494366/fase/brasileiro-serie-b-2026-fase-unica/rodada/{}/jogos/"

OUTPUT_FILE = "brasileirao_serie_b_2025.csv"

ultima_data = None

with open(OUTPUT_FILE, mode="w", newline="", encoding="utf-8") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["data", "mandante", "visitante"])

    for rodada in range(1, 39):
        url = BASE_URL.format(rodada)
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Erro ao obter dados da rodada {rodada}")
            continue

        jogos = response.json()
        for jogo in jogos:
            data_iso = jogo.get("data_realizacao")

            if data_iso:
                ultima_data = datetime.strptime(data_iso, "%Y-%m-%dT%H:%M")
            elif ultima_data:
                pass  # mantém a última data válida
            else:
                continue  # pula se ainda não temos nenhuma data válida

            data_formatada = ultima_data.strftime("%d/%m/%Y")
            mandante = jogo["equipes"]["mandante"]["nome_popular"]
            visitante = jogo["equipes"]["visitante"]["nome_popular"]
            writer.writerow([data_formatada, mandante, visitante])

print(f"Arquivo {OUTPUT_FILE} criado com sucesso!")
