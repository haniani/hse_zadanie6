import pymysql as db
import csv, os



class sql_worker:

    def __init__(self, USER, PASSWD, HOST, CHARSET, DATABASE, COLLATE):
        self.user = USER
        self.passwd = PASSWD
        self.host = HOST
        self.charset = CHARSET
        self.database = DATABASE
        self.collate = COLLATE

    def connect(self): #подключаемся 
        try:
            self.connection = db.connect(host=self.host, user=self.user, passwd=self.passwd, charset=self.charset)
        except:
            return False
        else:
            return True

    def create(self): #создаем базу
        try:
            with self.connection.cursor() as cursor:
                self.creation = cursor.execute("create database `%s` DEFAULT CHARSET=`%s` COLLATE=%s;" % (self.database, self.charset, self.collate))
        except:
            return False
        else:
            return True

    def use(self, db): #выбираем базу
        try:
            with self.connection.cursor() as cursor:
                self.use = cursor.execute("use `%s`;" % db)
        except:
            return False
        else:
            return True

    def create_table(self, tablename, fields): #создаем таблицу
        self.tbl = tablename
        self.fields = fields

        try:
            with self.connection.cursor() as cursor:
                self.use = cursor.execute("create table %s (%s) DEFAULT CHARSET=%s COLLATE=%s;" % (self.tbl, self.fields, self.charset, self.collate))
        except:
            return False
        else:
            return True

    def query(self, query):  #запрос
        try:
            with self.connection.cursor() as cursor:
                self.querys = cursor.execute(query)
                self.connection.commit()
        except:
            return False
        else:
            return self.querys

    def select(self, query): #запрос select
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
        except:
            return False
        else:
            return cursor.fetchone()

    def close(self): #закрываем соединение
        try:
            with self.connection.cursor() as cursor:
                cursor.close()
        except:
            return False
        else:
            return True




def main():

    #config

    USER = "root"
    PASSWD = "123456"
    HOST = "localhost"
    CHARSET = "utf8mb4"
    DATABASE = "vk"
    COLLATE = "utf8mb4_unicode_ci"
    #PORT = "3306"
    PATH = './'
    PATH_U = './users'


    x = sql_worker(USER, PASSWD, HOST, CHARSET, DATABASE, COLLATE)
    print(x.connect())
    print(x.create())
    print(x.use(DATABASE))
    print(x.create_table("users", "id INT(10) NOT NULL AUTO_INCREMENT, vk_usrid INT(10), vk_name VARCHAR(255), vk_sex VARCHAR(10), vk_bdate VARCHAR(10), vk_city VARCHAR(255), vk_country VARCHAR(255), vk_followers INT(10), PRIMARY KEY (id)"))
    print(x.create_table("posts", "id INT(10) NOT NULL AUTO_INCREMENT, vk_author_id INT(10), vk_post_id INT(10),  post TEXT, PRIMARY KEY (id)"))

    with open(PATH+'metainfo.csv', 'r', encoding='utf-8') as csv_file:
        info = csv.DictReader(csv_file, delimiter=',')
        for y in info:
            string = "\""+ str(y['id пользователя'])+ "\", " + "\""+ str(y['имя']) + "\", " + "\"" + str(y['пол']) + "\", "+ "\"" + str(y['дата рождения']) + "\", " + "\"" + str(y['город']) + "\", " + "\"" + str(y['страна']) + "\", " + "\"" + str(y['количество подписчиков']) + "\""
            print(string)
            a = x.query("insert into users (vk_usrid, vk_name, vk_sex, vk_bdate, vk_city, vk_country, vk_followers) VALUES (%s);" % string)
            print(a)

    ls = os.listdir(PATH_U)

    for folder in ls:

        if os.path.isdir(PATH_U+"/"+str(folder)) :
            posts = os.listdir(PATH_U+"/"+str(folder))
            for each in posts:
                with open(PATH_U+"/"+str(folder)+"/"+str(each), 'r', encoding='utf-8') as file:
                    strings = file.readlines()
                    strings = "\n".join(strings)

                    each = os.path.splitext(each)[0]

                    values = "\"" + str(folder) + "\"," + "\"" + str(each) + "\"," + "\"" + strings + "\""
                    x.query("insert into posts (vk_author_id, vk_post_id, post) VALUES (%s);" % values)



    print(x.select("select * from posts limit 10"))

    print(x.close())


if __name__ == '__main__':
    main()
