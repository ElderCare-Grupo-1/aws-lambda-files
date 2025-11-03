import json
import pymysql
import os
import requests

def lambda_handler(event, context):
    connection = pymysql.connect(
        host=os.environ['DB_HOST'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD'],
        database=os.environ['DB_NAME']
    )

    try:
        with connection.cursor() as cursor:
            sql = """
                INSERT INTO TBL_SENSORES 
                (sensor_id, data_hora, local, valor)
                VALUES (%s, %s, %s, %s)
            """
            
            cursor.execute(sql, (
                event['SensorId'],
                event['Timestamp'],
                event['Local'],
                event['Data']
            ))
            
            connection.commit()
            correlacao_cardiaca(event, connection)

    finally:
        connection.close()

    crawler_clima(event['Timestamp'])

    return {
        'statusCode': 201,
        'body': json.dumps('Dados inseridos no banco de dados!')
    }

def crawler_clima(dataHora):

    connection = pymysql.connect(
        host=os.environ['DB_HOST'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD'],
        database=os.environ['DB_NAME']
    )

    api_key = "cca7198fc5eb0b143055b5c6a6994d23"

    url = f"http://api.openweathermap.org/data/2.5/weather?q=Sao Paulo,Brazil&APPID={api_key}"

    data = requests.get(url).json()

    try:
        with connection.cursor() as cursor:
            sql = """
                INSERT INTO TBL_CRAWLER_CLIMA 
                (temp_atual, temp_min, temp_max, sensacao_termica, descricao_clima, data_hora)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(sql, (
                data.get('main').get('temp', 0) - 273.15,
                data.get('main').get('temp_min', 0) - 273.15,
                data.get('main').get('temp_max', 0) - 273.15,
                data.get('main').get('feels_like', 0) - 273.15,
                data.get('weather')[0].get('description', ''),
                dataHora
            ))
            
            connection.commit()
            
    finally:
        connection.close()

ultimo_valor = {
    101: None,
    103: None
}

def correlacao_cardiaca(event, connection):
    global ultimo_valor

    item = {
        'SensorId': event['SensorId'],
        'dataHora': event['Timestamp'],
        'valor': event['Data'],
        'Local': event['Local']
    }

    ultimo_valor[item['SensorId']] = item['valor']

    if ultimo_valor[101] is None or ultimo_valor[103] is None:
        return {
            'statusCode': 200,
            'body': 'Aguardando dados de ambos os sensores (101 e 103)'
        }

    spo2 = ultimo_valor[101]
    bpm = ultimo_valor[103]

    risco_spo2 = max(0, min(1, (100 - spo2) / 30))
    risco_bpm = max(0, min(1, abs(bpm - 80) / 60))
    
    peso_spo2 = 2.48
    peso_bpm = 1.0
    
    soma_pesos = peso_spo2 + peso_bpm
    peso_spo2 /= soma_pesos
    peso_bpm /= soma_pesos
    
    chance = 100 * (peso_spo2 * risco_spo2 + peso_bpm * risco_bpm)
    
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO TBL_CORRCARDIACA (chance, data_hora)
                VALUES (%s, %s)
            """, (chance, item['dataHora']))
        connection.commit()
    except Exception as e:
        print("Erro ao inserir dados de correlacao no banco de dados:", e)

    return {
        'statusCode': 201,
        'body': 'Dados de correlacao enviados para o MySQL!'
    }
