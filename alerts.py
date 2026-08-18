# alerts.py
import os
import smtplib
from email.mime.text import MIMEText

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SENDER_EMAIL = os.environ.get("ALERT_SENDER_EMAIL")
SENDER_APP_PASSWORD = os.environ.get("ALERT_SENDER_APP_PASSWORD")
RECIPIENT_EMAIL = os.environ.get("ALERT_RECIPIENT_EMAIL")

ERROR_RATE_THRESHOLD = 20.0  # % - dispara alerta acima disso


def send_alert(window_start, window_end, total, valid, invalid, error_rate, top_errors):
    if not all([SENDER_EMAIL, SENDER_APP_PASSWORD, RECIPIENT_EMAIL]):
        print("Alertas por e-mail não configurados (variáveis de ambiente ausentes) — pulando.")
        return

    body = f"""
Alerta de qualidade de dados — pipeline de streaming

Janela: {window_start.strftime('%H:%M:%S')} - {window_end.strftime('%H:%M:%S')}
Total de eventos: {total}
Válidos: {valid}
Inválidos: {invalid}
Taxa de erro: {error_rate}% (limite: {ERROR_RATE_THRESHOLD}%)

Principais motivos de erro:
{chr(10).join(f"  - {reason}: {count} ocorrências" for reason, count in top_errors)}
"""

    msg = MIMEText(body)
    msg["Subject"] = f"⚠️ Alerta: taxa de erro {error_rate}% no pipeline de e-commerce"
    msg["From"] = SENDER_EMAIL
    msg["To"] = RECIPIENT_EMAIL

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SENDER_EMAIL, SENDER_APP_PASSWORD)
            server.send_message(msg)
        print(f"Alerta enviado para {RECIPIENT_EMAIL}")
    except Exception as e:
        print(f"Falha ao enviar alerta: {e}")


def check_and_alert(window_start, window_end, total, valid, invalid, error_rate, top_errors):
    if error_rate >= ERROR_RATE_THRESHOLD:
        send_alert(window_start, window_end, total, valid, invalid, error_rate, top_errors)