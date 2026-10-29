import json, time, random
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

ciudades = ['Cali', 'Bogota', 'Medellin', 'Barranquilla', 'Yumbo']
medios_pago = ['PSE', 'Tarjeta_Credito', 'Tarjeta_Debito', 'Efectivo']
transaccion_id = 1000

while True:
    transaccion_id += 1
    data = {
        'transaccion_id': f"TX-{transaccion_id}",
        'monto': round(random.uniform(10000, 250000), 2),
        'medio_pago': random.choice(medios_pago),
        'ciudad': random.choice(ciudades),
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    producer.send('transacciones_topic', value=data)
    print(f"[ENVIADO] ID: {data['transaccion_id']} | Monto: ${data['monto']:,.2f} | Ciudad: {data['ciudad']}")
    time.sleep(2)