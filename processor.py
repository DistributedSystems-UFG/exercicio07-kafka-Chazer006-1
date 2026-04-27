from kafka import KafkaConsumer, KafkaProducer
from const import *
import sys

# Verifica argumentos: tópico de entrada e tópico de saída
try:
    input_topic = sys.argv[1]
    output_topic = sys.argv[2]
except IndexError:
    print('Usage: python3 processor.py <input_topic> <output_topic>')
    exit(1)

# Inicializa o Consumidor para ler o primeiro tópico
consumer = KafkaConsumer(
    input_topic,
    bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT],
    auto_offset_reset='earliest' # Garante que processamos desde o início
)

# Inicializa o Produtor para publicar no segundo tópico
producer = KafkaProducer(
    bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT]
)

print(f"Processando de {input_topic} para {output_topic}...")

for msg in consumer:
    # 1. Recebe o evento (decodifica de bytes para string)
    input_value = msg.value.decode('utf-8')
    print(f"Recebido de {input_topic}: {input_value}")

    # 2. Processa o evento (exemplo: transformando em letras maiúsculas)
    processed_value = input_value.upper() + " [PROCESSADO]"
    
    # 3. Produz o novo evento no tópico de saída
    print(f"Enviando para {output_topic}: {processed_value}")
    producer.send(output_topic, value=processed_value.encode('utf-8'))
    
    # Opcional: Flush para garantir envio imediato
    producer.flush()