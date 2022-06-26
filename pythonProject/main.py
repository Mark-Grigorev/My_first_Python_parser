import sqlite3
import dateutil.utils
import psycopg2  # Библиотека для работы с PostgreSQL
from datetime import datetime

def execute_query(connection, query):  # ф-я для запросов и т.п. в БД
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Query executed successfully")
    except sqlite3.OperationalError as e:
        print(f"The error '{e}' occurred")
def data_separation_deps(a, b, c): # Ф-я для разделения данных(id / dep-s title)
    i = 0
    while i < len(a):
        if i == 0:
            b.append(a[i])
            i += 1

        if i % 2 == 0:
            b.append(a[i])
            i += 1

        else:
            c.append(a[i])
            i += 1
    return b,c
def time_1(date_text):
       try:
           datetime.strptime(date_text,'%Y-%m-%d')
       except ValueError:
            date_text = dateutil.utils.today()
       return date_text
#Функция, для обработки данных, и их дальнейшая подготовка для записи в базу
def data_separation_empl(array,arr):
    txt = ''
    for text in array:
        x = text.split(',')
        if len(x) == 10:
            x[0] = x[0].replace('"', '')
            x[9] = x[9].replace('"', '')
            txt = x[6] + x[7]
            arr.append(x[0])
            arr.append(x[1])
            arr.append(x[2])
            arr.append(x[3])
            arr.append(x[4])
            arr.append(x[5])
            arr.append(txt)
            arr.append(x[8])
            arr.append(x[9])
        else:
            x[0] = x[0].replace('"', '')
            x[8] = x[8].replace('"', '')
            arr.append(x[0])
            arr.append(x[1])
            arr.append(x[2])
            arr.append(x[3])
            arr.append(x[4])
            arr.append(x[5])
            arr.append(x[6])
            arr.append(x[7])
            arr.append(x[8])
    return arr


con = psycopg2.connect(  # Коннект с БД
    database="1work_PR",  # Имя БД, к которой подключаемся
    user="postgres",  # Имя пользователя в самой бд, для аутентификации
    password="1",  # Пароль от самой БД
    host="127.0.0.1",  # Адрес сервера БД
    port="5432")  # Номер порта
cursor = con.cursor()
print("Database opened successfully")  # Вывод сообщения, при удачном подключении

departaments = []  # Список для данных из файла
departaments_id = [] #Список для id
departaments_title = [] #Список для названий депортаментов

f = open('CSV_files/departments.csv', 'r')  # Сбор данных, для парсинга в БД
departaments = f.read().split(',')  # Заносим данные в список, разделяя запятой

index_id_dep = departaments.index("id")  # Удаляем оглавниение ID/Deportament_title
removed_id = departaments.pop(index_id_dep)
index_dep_inf = departaments.index("department_title")
removed_dep = departaments.pop(index_dep_inf)

data_separation_deps(departaments,departaments_id,departaments_title) #Передача данных в функцию
#Не удалять!!!
 #Заполнение БД данными  Deportments
p = (len(departaments)/2)-1             #Урезаем кол-во строк(-1, т.к. счет с 0)
print("len p = ",p)
i=0
while i<p:
    k = departaments_id[i]
    l = departaments_title[i]
    i += 1
    print("id = ", k," title = ",l)
    cursor.execute("INSERT INTO departments VALUES(%s,%s)", (k,l))
# con.commit()
# cursor.close()
# con.close() #Не

# Обработка данных таблицы Employees
f = open('CSV_files/employees.csv', 'r')  # Сбор данных, для парсинга в БД
employees = f.read().split("\n")
print("empl len",len(employees))
del employees[0]                                                     #Удаляем огравление
o = len(employees)
del employees[o-1]   #Удаляем лишнюю пустую строку данных
arr = []
data_separation_empl(employees,arr)
a =0
while a<len(arr):
    count = 0
    id = arr[a]
    fname = arr[a+1]
    lname = arr[a+2]
    email = arr[a+3]
    dept_id= arr[a+4]
    city = arr[a+5]
    country= arr[a+6]
    coutry_cd = arr[a+7]
    date =arr[a+8]
    cursor.execute("INSERT INTO employees VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                                  (id,fname,lname,email,dept_id,city,country,coutry_cd,date))
    a+=9
con.commit()
cursor.close()
con.close()
