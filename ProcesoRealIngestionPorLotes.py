# Import modules 
import sqlite3 
import time
import random

# Conectando con sqlite 
conn = sqlite3.connect('RealIngestionLotes.db') 

# Creando un objeto del tipo cursor
cursor = conn.cursor() 

# Creando una tabla 
table ="""CREATE TABLE EJEMPLO(id INTEGER, value INTEGER, transformed_value INTEGER);"""
cursor.execute(table) 

# Generando datos de prueba
def generate_mock_data(num_records):
    data = []
    for _ in range(num_records):
        record = {
            'id': random.randint(1, 1000),
            'value': random.randint(50000, 500000)
        }
        data.append(record)
    return data

# Creando la separación por lotes (batch)
def process_in_batches(data, batch_size):
    for i in range(0, len(data), batch_size):
        yield data[i:i + batch_size]

# Transformando datos de prueba
def transform_data(batch):
    transformed_batch = []
    for record in batch:
        transformed_record = {
            'id': record['id'],
            'value': record['value'],
            'transformed_value': record['value'] * 2  # ejemplo de transformación
        }
        transformed_batch.append(transformed_record)
    return transformed_batch

# Cargando los datos
def load_data(batch):
    for record in batch:
        # Simulate loading data into a database
        print(f"Loading record into database: {record}")

# función Main
def main():
    # parametros
    num_records = 1000  # Total number of records to generate
    batch_size = 10    # Number of records per batch

    # Generando los datos de prueba
    data = generate_mock_data(num_records)
    print("Original data:",data)
    
    # Procesando y cargando los lotes
    for batch in process_in_batches(data, batch_size):
        transformed_batch = transform_data(batch)
        #print("Batch before loading:")
        for record in transformed_batch:
            for x,y in record.items(): 
                if x == "id":
                    #print(y)
                    v1 = y
                if x == "value":
                    #print(y)
                    v2 = y
                if x == "transformed_value":
                    #print(y)
                    v3 = y

            cursor.execute("""INSERT INTO EJEMPLO (id, value, transformed_value)""" + """ VALUES (""" + str(v1) + ", " + str(v2) + ", " + str(v3) + """)""") 

            # Comsolidando los cambios en la bd	 
            conn.commit() 

        load_data(transformed_batch)
        time.sleep(1)  # Simulate time delay between batches


if __name__ == "__main__":
    main()



# Cerrando la conexion de la bd 
conn.close()

