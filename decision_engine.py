import logging
import json
import os
from datetime import datetime, timedelta

from execution import send_telegram_alert

log = logging.getLogger(__name__)

class DecisionEngine:
    def __init__(self, required_confirmations: int = 2, cooldown_minutes: int = 15):
        self.required_confirmations = required_confirmations
        self.cooldown_minutes = cooldown_minutes
        
        # State: { "BTC/USDT": {"direccion": "LONG", "timestamp": ...} }
        self.active_trades = {}
        
        # Debouncing: { "BTC/USDT": {"direccion": "LONG", "count": 1} }
        self.pending_signals = {}
        
        # Cooldowns: { "BTC/USDT": datetime }
        self.cooldowns = {}
        
        self.state_file = "output/state.json"
        self.history_file = "output/signals_history.jsonl"
        self._load_state()

    def _load_state(self):
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.active_trades = data.get("active_trades", {})
            except Exception as e:
                log.error(f"[ENGINE] Error cargando estado: {e}")

    def _save_state(self):
        # Aseguramos que el directorio exista
        os.makedirs(os.path.dirname(self.state_file), exist_ok=True)
        try:
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump({"active_trades": self.active_trades}, f, indent=2)
        except Exception as e:
            log.error(f"[ENGINE] Error guardando estado: {e}")

    def process_signal(self, data: dict):
        self._log_signal_to_file(data)
        
        activo = data.get("activo", "").upper()
        direccion = data.get("direccion", "NEUTRAL").upper()
        
        if not activo or direccion not in ["LONG", "SHORT"]:
            return

        now = datetime.now()
        
        # Check cooldown
        if activo in self.cooldowns:
            if now < self.cooldowns[activo]:
                log.debug(f"[ENGINE] Ignorando {activo} (En cooldown por {self.cooldown_minutes} mins)")
                return
            else:
                del self.cooldowns[activo]
                
        # Check shield: Already in this trade?
        if activo in self.active_trades:
            trade_actual = self.active_trades[activo].get("direccion")
            if trade_actual == direccion:
                log.info(f"🛡️ [ESCUDO] Ya estamos {direccion} en {activo}. Señal repetida ignorada.")
                # Activamos el cooldown para no seguir evaluando lo mismo
                self.cooldowns[activo] = now + timedelta(minutes=self.cooldown_minutes)
                return
            else:
                # Señal opuesta => Cerramos el trade actual
                self._execute_order("CERRAR", trade_actual, activo, data)
                del self.active_trades[activo]
                self._save_state()
                if activo in self.pending_signals:
                    del self.pending_signals[activo]

        # Debouncing (confirmaciones)
        pending = self.pending_signals.get(activo, {"direccion": None, "count": 0})
        if pending["direccion"] == direccion:
            pending["count"] += 1
        else:
            pending = {"direccion": direccion, "count": 1}
        
        self.pending_signals[activo] = pending
        
        log.info(f"🚦 [ENGINE] {activo} -> {direccion} (Confirmación {pending['count']}/{self.required_confirmations})")
        
        # Ejecutar orden si cumple confirmaciones
        if pending["count"] >= self.required_confirmations:
            self._execute_order("ABRIR", direccion, activo, data)
            self.active_trades[activo] = {
                "direccion": direccion,
                "timestamp": now.isoformat()
            }
            self._save_state()
            # Cooldown después de entrar para evitar re-entradas inmediatas
            self.cooldowns[activo] = now + timedelta(minutes=self.cooldown_minutes)
            del self.pending_signals[activo]

    def _execute_order(self, accion: str, direccion: str, activo: str, context: dict):
        icono = "🟢" if direccion == 'LONG' else "🔴"
        if accion == "CERRAR":
            icono = "🏁"
            
        precio = context.get("precio_entrada", 0)
        sl = context.get("stop_loss", 0)
        tp = context.get("take_profit", 0)
        razon = context.get("razon_tecnica", "N/A")
        
        msg = f"\n{icono} [TRADE ALERT] {accion} {direccion} | {activo}\n"
        if accion == "ABRIR":
            msg += f"💰 Entrada: {precio} | SL: {sl} | TP: {tp}\n"
        msg += f"🧠 Razón: {razon}\n"
        
        log.info(msg)
        
        # Enviar alerta a Telegram
        send_telegram_alert(accion, direccion, activo, context)

    def _log_signal_to_file(self, data: dict):
        try:
            os.makedirs(os.path.dirname(self.history_file), exist_ok=True)
            data['timestamp'] = datetime.now().isoformat()
            with open(self.history_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(data, ensure_ascii=False) + '\n')
        except Exception:
            pass
