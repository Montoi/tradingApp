import logging
from decision_engine import DecisionEngine
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(message)s')

def run_test():
    # Eliminar estado anterior si existe para un test limpio
    if os.path.exists("output/state.json"):
        os.remove("output/state.json")
        
    # Usamos cooldown de 0 para poder probar en un solo script, o forzamos señales fuera del cooldown
    engine = DecisionEngine(required_confirmations=2, cooldown_minutes=15)
    
    signals = [
        {"activo": "BTC/USDT", "direccion": "NEUTRAL", "razon_tecnica": "Nada interesante"},
        {"activo": "BTC/USDT", "direccion": "LONG", "precio_entrada": 65000, "stop_loss": 64000, "take_profit": 67000, "razon_tecnica": "Vela martillo"},
        {"activo": "BTC/USDT", "direccion": "LONG", "precio_entrada": 65050, "stop_loss": 64000, "take_profit": 67000, "razon_tecnica": "Vela confirmada"}, # CONFIRMA Y ABRE LONG
        {"activo": "BTC/USDT", "direccion": "LONG", "razon_tecnica": "Spam del streamer"}, # DEBE SER IGNORADA POR EL ESCUDO
        {"activo": "ETH/USDT", "direccion": "SHORT", "precio_entrada": 3000, "stop_loss": 3100, "take_profit": 2800, "razon_tecnica": "Caída libre"},
        {"activo": "ETH/USDT", "direccion": "SHORT", "precio_entrada": 2990, "stop_loss": 3100, "take_profit": 2800, "razon_tecnica": "Sigue cayendo"}, # CONFIRMA Y ABRE SHORT EN ETH
    ]
    
    print("Iniciando prueba del Motor de Decisiones (Escudo y Persistencia)...")
    for i, s in enumerate(signals):
        print(f"\n--- Señal {i+1}: {s.get('activo')} -> {s.get('direccion')} ---")
        engine.process_signal(s)
        
    print("\nEstado final guardado en memoria:", engine.active_trades)

if __name__ == '__main__':
    run_test()
