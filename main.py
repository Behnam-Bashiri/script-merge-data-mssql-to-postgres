import pyodbc
import psycopg2

def ConverterDB(mssqlInfo={},postgresInfo={}):
    try:

        # connect to mssql
        if mssqlInfo['port']:server = mssqlInfo['server']+','+mssqlInfo['port']
        else:server = mssqlInfo['server']
        database = mssqlInfo['database']
        username = mssqlInfo['username']
        password = mssqlInfo['password']
        cnxn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+password
        )
        print("mssql connected")
        cursor_mssql = cnxn.cursor()
        cursor_mssql.execute('SELECT * FROM {}'.format(mssqlInfo['table']))


        # connect to postgres
        conn = psycopg2.connect(
            host=postgresInfo['server'],
            port=postgresInfo['port'],
            database=postgresInfo['database'],
            user=postgresInfo['username'],
            password=postgresInfo['password']
        )
        print("postgres connected")
        cursor_post = conn.cursor()

        # get column name
        column_name_table_qury = '''SELECT * FROM {}'''.format(postgresInfo['table'])
        cursor_post.execute(column_name_table_qury)
        column_names = [desc[0] for desc in cursor_post.description]
        result_column = ''
        column_count = 0
        for col in column_names: 
            result_column+="\"{}\", ".format(col)
            column_count += 1

        result_column = result_column[:-2]
        typedate_str = ''
        for i in range(column_count):
            typedate_str += "%s,"
        typedate_str = typedate_str[:-1]

        # push data
        for item in cursor_mssql:
            row_to_list = [elem for elem in item]
            synatxINSERT ="""INSERT INTO {}({}) VALUES ({})""".format(postgresInfo['table'],result_column,typedate_str)
            
            # add timezone !
            temp_time = row_to_list[1]
            temp_date = row_to_list[0]
            temp_time = "{}T{}".format(temp_date,temp_time)
            values = (row_to_list[0],temp_time,row_to_list[2],row_to_list[3],row_to_list[4],row_to_list[5],row_to_list[6],row_to_list[7],row_to_list[8],row_to_list[9])
            cursor_post.execute(synatxINSERT,values)
            
        conn.commit()
        print("Succefully insert data to postgres")

    except (Exception,pyodbc.Error,psycopg2.Error) as error:
        print("Failed insert data to postgres\nError : {}".format(error))

    finally:
        if conn and cnxn:
            cursor_mssql.close()
            cursor_post.close()
            conn.close()
            cnxn.close()
            print("Connection is closed !")



if __name__ == "__main__":
    # config mssql
    mssqlInfo = {
        'server':'' ,   #Server MSSQL
        'port':'',      #Port connection MSSQL default : 1433
        'database':'' , #Database name MSSQL
        'username':'' , #Username MSSQL
        'password':'' , #Password MSSQL
        'table':''      #Table selected to copy
    }

    # config postgres
    postgresInfo = {
        'server':'' ,   #Server Postgres
        'port':'5432',  #Port connection Postgres default : 5432
        'database':'' , #Database name Postgres
        'username':'' , #Username Postgres
        'password':'' , #Password Postgres
        'table':"public."+"\"\"",     #Table selected to Postgres EX(public."logData_logweathermeteo")
    }

    ConverterDB(mssqlInfo,postgresInfo)


