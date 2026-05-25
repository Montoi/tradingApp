import os
import requests
import logging

log = logging.getLogger(__name__)

def send_telegram_alert(accion: str, direccion: str, activo: str, context: dict):
    """
    Envía un mensaje de alerta a Telegram con formato MarkdownV2.
    """
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    
    if not token or not chat_id:
        log.warning("⚠️ No se puede enviar alerta de Telegram: Faltan las variables de entorno TELEGRAM_BOT_TOKEN o TELEGRAM_CHAT_ID")
        return

    # Preparar el icono y el texto
    icono = "🟢" if direccion.upper() == 'LONG' else "🔴"
    if accion.upper() == "CERRAR":
        icono = "🏁"
        
    precio = context.get("precio_entrada", "N/A")
    sl = context.get("stop_loss", "N/A")
    tp = context.get("take_profit", "N/A")
    razon = context.get("razon_tecnica", "Sin razón técnica")

    # Formatear el mensaje en HTML
    msg = f"<b>{icono} [TRADE ALERT] {accion.upper()} {direccion.upper()} | {activo}</b>\n\n"
    if accion.upper() == "ABRIR":
        msg += f"💰 <b>Entrada:</b> <code>{precio}</code>\n"
        msg += f"🛡️ <b>SL:</b> <code>{sl}</code>\n"
        msg += f"🎯 <b>TP:</b> <code>{tp}</code>\n\n"
    
    msg += f"🧠 <b>Razón:</b> <i>{razon}</i>"

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": msg,
        "parse_mode": "HTML"
    }

    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        log.info(f"✅ Alerta de Telegram enviada exitosamente para {activo}")
    except Exception as e:
        log.error(f"❌ Error al enviar alerta a Telegram: {e}")
