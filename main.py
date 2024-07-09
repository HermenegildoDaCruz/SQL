import pymysql as sql
import pymysql.cursors as cursors
import os
import dotenv
from typing import cast


dotenv.load_dotenv()
TABLE_NAME = 'users'
CURSOR_TYPE = cursors.DictCursor
connection = sql.connect(
   host=os.environ['MYSQL_HOST'],
   user=os.environ['MYSQL_USER'],
   password=os.environ['MYSQL_PASSWORD'],
   database=os.environ['MYSQL_DATABASE'],
   cursorclass= CURSOR_TYPE #Usando um dict cursor para retornar dicionários
)

with connection:
   with connection.cursor() as cursor:
      #SQl
      cursor.execute(
         f'CREATE TABLE IF NOT EXISTS {TABLE_NAME} ('
         'id INT NOT NULL AUTO_INCREMENT, '
         'name VARCHAR(30) NOT NULL, '
         'weight REAL NOT NULL, '
         'PRIMARY KEY (id)'
         ')'
      )

   with connection.cursor() as cursor:
      sql = (F'TRUNCATE TABLE {TABLE_NAME}')
      cursor.execute(sql)
      connection.commit()
      sql = (f'INSERT INTO {TABLE_NAME} '
             '(name, weight) '
             'VALUES'
            '(%s,%s) ')
      #INSERINDO DADOS NA TABELA USANDO LISTA DE TUPLAS
      datas = [
         ('João',67),
         ('Mauro', 89),
         ('Gustavo',70)
      ]

      for data in datas:
         cursor.execute(sql, data)


      
      connection.commit()

      #INSERINDO LISTA DE DICIONÁRIOS(VALORES) NA TABELA USERS:
   with connection.cursor() as cursor:

      
      sql = (f'INSERT INTO {TABLE_NAME} '
            '(name, weight) '
            'VALUES'
            '(%(nome)s,%(peso)s) ')
      
      datas2 = [
         {"nome": "Maria", "peso":78},
         {"nome": "Joana", "peso":54},
         {"nome": "Tiago", "peso":86},
      ]

      cursor.executemany(sql, datas2)# O executemany pega cada elemento da lista, ou seja cada dicionário
      

      connection.commit()

   #LENDO DADOS COM SELECT:
   with connection.cursor() as cursor:

      """
      sql = (f'SELECT * FROM {TABLE_NAME} ')
      cursor.execute(sql)

      #Iterando na tupla de tuplas do cursor.fetchall
      for row in cursor.fetchall():
         print(row)"""

   #Usando placeholders com select para evitar sql injection, defini o tipo de dados do id_recebido para maior segurança
   with connection.cursor() as cursor:
      #id_recebido = int(input('Digite o id: '))

       ...
      #sql = (f'SELECT * FROM {TABLE_NAME} '
      #         'WHERE id = %s'
      #         )
      #cursor.execute(sql, (id_recebido))
      #print(cursor.mogrify(sql,(id_recebido)))# O mogrify mostra o comando completo com o valor para por alguém já substituido no placeholder

      #for row in cursor.fetchall():
       #  print(row)

   #DELETE COM WHERE + PLACEHOLDER
   with connection.cursor() as cursor:
      
   
      sql = (f'DELETE FROM {TABLE_NAME} '
               'WHERE id = %s'
               )
      cursor.execute(sql, (4))
      connection.commit()
      
      cursor.execute(f'SELECT * FROM {TABLE_NAME}')
      #for row in cursor.fetchall():
         #print(row)

   #UPDATE COM WHERE + PLACEHOLDER
   with connection.cursor() as cursor:
      
   
      sql = (f'UPDATE {TABLE_NAME} '
             'SET name = %s, weight = %s '
               'WHERE id = %s'
               )
      cursor.execute(sql,('Hopson', 62.3, 6))
      connection.commit()
      
      cursor.execute(f'SELECT * FROM {TABLE_NAME}')
      """
      for row in cursor.fetchall():
         print(row)

      cursor.scroll(0, 'absolute') # Esse trecho vai posicionar o cursor devolta a posição inicial ou no índice 0 do iterável
      print()
      for row in cursor.fetchall():
         print(row)"""

   #Usando o SSDictCursor para receber um dado de cada vez(generator) para lidar com um grande volume de dados
   #Código incompleto, verifar no github do curso de python
   with connection.cursor() as cursor:
      
      cursor = cast(CURSOR_TYPE, cursor)# Definindo a tipagem do cursor

      resultFromSelect = cursor.execute(f'SELECT * FROM {TABLE_NAME}') #Essa variável foi criada para ver quantas linhas foram afetada pelo select
      """ Esse trecho só funciona com um SScursor(generator), se estiver aparecendo erros verfique o tipo de cursor
      print('For 1')
      for row in cursor.fetchall_unbuffered():
         print(row)

         if row['id'] >= 5:
            break"""

      datas3 = cursor.fetchall()#Lista de valores da tabela users
      for row in datas3:
         #print(row)
         ...

      #print(resultFromSelect)# Mostrando quantas linhas foram afetadas com a variável resultFromSelect
      #print(len(datas3)) # Mostrando quantas linhas foram afetas ou retornadas usando a função len()

   connection.commit()
   
   #Roucount, rownumber and lastrowid
   with connection.cursor() as cursor:
      
      cursor = cast(CURSOR_TYPE, cursor)# Definindo a tipagem do cursor
      cursor.execute(f'SELECT id FROM {TABLE_NAME} ORDER BY id DESC LIMIT 1')#Selecionando a última linha da tabela users
      lastrowid = cursor.fetchone() #Pegando o valor selecionado com o fetchone(Serve para pegar apenas um valor)
      print('Lastrowid: ',lastrowid)
      cursor.scroll(-1)#Andando uma linha para atrás com o cursor
      
      print('rowcount', cursor.rowcount)# posicao do cursor(linha em que se encontra)
      print('rownumber(Posicao do cursor): ', cursor.rownumber)
   
   connection.commit()
      
     


         


      