import pandas as pd # type: ignore
from pymongo import MongoClient # type: ignore

# Función para insertar datos en MongoDB
def insertar_datos():
    file_path = 'dataset-salary.csv'
    data = pd.read_csv(file_path)

    client = MongoClient('mongodb://localhost:27017/')
    db = client['dev_salary_db']
    collection = db['salaries']

    collection.drop()
    data.reset_index(inplace=True)
    data_dict = data.to_dict("records")
    collection.insert_many(data_dict)

    print("Datos insertados correctamente en MongoDB")

# Función para realizar las consultas
def realizar_consulta(opcion): 
    client = MongoClient('mongodb://localhost:27017/')
    db = client['dev_salary_db']
    collection = db['salaries']

    if opcion == 1:
        realizar_consulta_opcion_1(collection)
    
    elif opcion == 2:
        realizar_consulta_opcion_2(collection)
    
    elif opcion == 3:
        realizar_consulta_opcion_3(collection)
    
    elif opcion == 4:
        realizar_consulta_opcion_4(collection)
    
    elif opcion == 5:
        realizar_consulta_opcion_5(collection)
    
    elif opcion == 6:
        print("Volviendo al menú principal...")

def realizar_consulta_opcion_1(collection):
    result = collection.aggregate([
        {"$group": {"_id": "$experience_level", "average_salary": {"$avg": "$salary_in_usd"}}}
    ])
    for r in result:
        print(r)

def realizar_consulta_opcion_2(collection):
    result = collection.aggregate([
        {"$group": {"_id": {"company_location": "$company_location", "experience_level": "$experience_level"}, "average_salary": {"$avg": "$salary_in_usd"}}},
        {"$sort": {"average_salary": -1}}
    ])
    for r in result:
        print(r)

def realizar_consulta_opcion_3(collection):
    result = collection.aggregate([
        {"$group": {"_id": "$company_location", "average_salary": {"$avg": "$salary_in_usd"}}},
        {"$sort": {"average_salary": -1}}
    ])
    for r in result:
        print(r)

def realizar_consulta_opcion_4(collection):
    result = collection.aggregate([
        {"$group": {"_id": None, "average_salary": {"$avg": "$salary_in_usd"}}}
    ])
    for r in result:
        print(r)

def realizar_consulta_opcion_5(collection):
    result = collection.aggregate([
        {"$group": {"_id": "$job_title", "average_salary": {"$avg": "$salary_in_usd"}}},
        {"$sort": {"average_salary": -1}}
    ])
    for r in result:
        print(r)

# Función principal con el menú
def menu():
    while True:
        print("\nMenú de Consultas de Salarios")
        print("1. Diferencia de salario entre los niveles de desarrolladores")
        print("2. Lugares con mayor salario según nivel de experiencia")
        print("3. ¿USA es el país con mejores salarios?")
        print("4. Salario promedio global")
        print("5. Área informática con mayor sueldo")
        print("6. Volver al menú principal")
        print("7. Insertar datos en MongoDB")
        print("8. Salir")

        opcion = int(input("Seleccione una opción: "))

        if opcion == 7:
            insertar_datos()
        elif opcion == 8:
            print("Saliendo...")
            break
        else:
            realizar_consulta(opcion)

if __name__ == "__main__":
    menu()
