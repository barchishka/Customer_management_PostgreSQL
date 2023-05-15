from pprint import pprint

import psycopg2


def delete_db(cur):
    cur.execute("""
        DROP TABLE number_client;
        DROP TABLE client;
        """)


# 1. Функция структуры БД (таблицы)
def create_db(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS client(
        id SERIAL PRIMARY KEY,
        name VARCHAR(30) NOT NULL,
        lastname VARCHAR(30),
        email VARCHAR(254) NOT NULL
        );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS number_client(
        id SERIAL PRIMARY KEY,
        number VARCHAR(11),
        client_id INTEGER REFERENCES client(id)
        );
    """)
    return


# 2. Функция добавления нового клиента.
def insert_tel(cur, id, tel):
    pass


def insert_client(cur, name=None, surname=None, email=None, tel=None):
    cur.execute("""
        INSERT INTO client(name, lastname, email)
        VALUES (%s, %s, %s)
        """, (name, surname, email))
    cur.execute("""
        SELECT id from client
        ORDER BY id DESC
        LIMIT 1
        """)
    id = cur.fetchone()[0]
    if tel is None:
        return id
    else:
        insert_tel(cur, id, tel)
        return id


# 3. Функция добавления номера телефона существующему клиенту
def insert_tel(cur, client_id, tel):
    cur.execute("""
        INSERT INTO number_client(number, client_id)
        VALUES (%s, %s)
        """, (tel, client_id))
    return client_id


# 4. Функция изменения данных клиента.
def update_client(cur, id, name=None, surname=None, email=None):
    cur.execute("""
        SELECT * from client
        WHERE id = %s
        """, (id,))
    info = cur.fetchone()
    if name is None:
        name = info[1]
    if surname is None:
        surname = info[2]
    if email is None:
        email = info[3]
    cur.execute("""
        UPDATE client
        SET name = %s, lastname = %s, email =%s
        where id = %s
        """, (name, surname, email, id))
    return id


# 5. Функция удаления телефона у клиента
def delete_phone(cur, number):
    cur.execute("""
        DELETE FROM number_client
        WHERE number = %s
        """, (number,))
    return number


# 6. Функция удаления клиента
def delete_client(cur, id):
    cur.execute("""
        DELETE FROM number_client
        WHERE client_id = %s
        """, (id,))
    cur.execute("""
        DELETE FROM client
        WHERE id = %s
       """, (id,))
    return id


# 7. Функция нахождения клиента по его данным: имени, фамилии, email или телефону
def find_client(cur, name=None, surname=None, email=None, tel=None):
    if name is None:
        name = '%'
    else:
        name = '%' + name + '%'
    if surname is None:
        surname = '%'
    else:
        surname = '%' + surname + '%'
    if email is None:
        email = '%'
    else:
        email = '%' + email + '%'
    if tel is None:
        cur.execute("""
            SELECT c.id, c.name, c.lastname, c.email, nc.number
            FROM client AS c
            LEFT JOIN number_client AS nc
                ON c.id = nc.client_id
            WHERE c.name LIKE %s
                AND c.lastname LIKE %s
                AND c.email LIKE %s
            """, (name, surname, email))
    else:
        cur.execute("""
            SELECT c.id, c.name, c.lastname, c.email, nc.number
            FROM client AS c
            LEFT JOIN number_client AS nc
                ON c.id = nc.client_id
            WHERE c.name LIKE %s
                AND c.lastname LIKE %s
                AND c.email LIKE %s
                AND nc.number like %s
            """, (name, surname, email, tel))
    return cur.fetchall()


if __name__ == '__main__':
    with psycopg2.connect(
            database="clients_db",
            user="postgres",
            password="******"
    ) as conn:
        with conn.cursor() as curs:
            # Удаление таблиц перед запуском
            delete_db(curs)

            # 1. Cоздание таблиц
            create_db(curs)
            print("БД создана")

            # 2. Добавление клиентов
            print("Добавлен клиент id: ",
                  insert_client(curs, "Андрей", "Просветов", "prosvetov@mail.ru"))

            print("Добавлен клиент id: ",
                  insert_client(curs, "Евгений", "Бормотов", "bormotov@gmail.com"))

            print("Добавлен клиент id: ",
                  insert_client(curs, "Дмитрий", "Новиков", "novik@yandex.ru", 86863214452))

            print("Добавлен клиент id: ",
                  insert_client(curs, "Олег", "Потапов", "potap@mail.ru", 86862645521))

            print("Добавлена клиент id: ",
                  insert_client(curs, "Роман", "Орлов", "orlov@outlook.com", 86864326871))

            print("Данные в таблицах")
            curs.execute("""
                SELECT c.id, c.name, c.lastname, c.email, nc.number
                FROM client AS c
                LEFT JOIN number_client AS nc
                    ON c.id = nc.client_id
                ORDER by c.id
                """)
            pprint(curs.fetchall())

            # 3. Добаление номера телефона
            print("Телефон добавлен клиенту id: ",
                  insert_tel(curs, 1, 86852356613))

            print("Телефон добавлен клиенту id: ",
                  insert_tel(curs, 2, 86853455598))

            print("Данные в таблицах")
            curs.execute("""
                SELECT c.id, c.name, c.lastname, c.email, nc.number
                FROM client AS c
                LEFT JOIN number_client AS nc
                    ON c.id = nc.client_id
                ORDER by c.id
                """)
            pprint(curs.fetchall())

            # 4. Изменение данных клиента
            print("Изменены данные клиента id: ",
                  update_client(curs, 3, "Сергей", None, "sery@mail.ru"))

            # 5. Удаление номера телефона
            print("Данный номер удален: ",
                  delete_phone(curs, "86864326871"))
            print("Данные в таблицах")
            curs.execute("""
                SELECT c.id, c.name, c.lastname, c.email, nc.number
                FROM client AS c
                LEFT JOIN number_client AS nc
                ON c.id = nc.client_id
                ORDER by c.id
                """)
            pprint(curs.fetchall())

            # 6. Удаление клиента из базы
            print("Клиент удалён с id: ",
                  delete_client(curs, 5))
            curs.execute("""
                SELECT c.id, c.name, c.lastname, c.email, nc.number
                FROM client AS c
                LEFT JOIN number_client AS nc
                ON c.id = nc.client_id
                ORDER by c.id
                """)
            pprint(curs.fetchall())

            # 7. Поиск клиента
            print('Найден клиент по имени:')
            pprint(find_client(curs, 'Андрей'))

            print('Найден клиент по email:')
            pprint(find_client(curs, None, None, 'novik@yandex.ru'))

            print('Найден клиент по имени, фамилии и email:')
            pprint(find_client(curs, 'Олег', 'Потапов', 'potap@mail.ru'))

            print('Найден клиент по имени, фамилии, email и телефон:')
            pprint(find_client(curs, 'Роман', 'Орлов', 'orlov@outlook.com', '86864326871'))

            print('Найден клиент по телефону:')
            pprint(find_client(curs, None, None, None, '86863214452'))
