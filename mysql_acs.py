class MysqlAcs:
    def __init__(self, engine):
        self.engine = engine

    def insertIntoManyMySQL(self, InsertStatement, MultipleRowsData):
        try:
            connection = self.engine.raw_connection()
            cursor = connection.cursor()
            cursor.executemany(InsertStatement, MultipleRowsData)
            cursor.close()
            connection.commit()
        finally:
            connection.close()

    def insertIntoMySQL(self, InsertStatement, RowsData):
        try:
            connection = self.engine.raw_connection()
            cursor = connection.cursor()
            cursor.execute(InsertStatement, RowsData)
            cursor.close()
            connection.commit()
        finally:
            connection.close()

    def deleteTableMySQL(self, DeleteStatement):
        try:
            connection = self.engine.raw_connection()
            cursor = connection.cursor()
            cursor.execute(DeleteStatement)
            cursor.close()
            connection.commit()
        finally:
            connection.close()

    def dropTableIfExistsMySQL(self, TableName):
        sql = "DROP TABLE IF EXISTS " + TableName
        try:
            connection = self.engine.raw_connection()
            cursor = connection.cursor()
            cursor.execute(sql)
            cursor.close()
            connection.commit()
        finally:
            connection.close()

    def createTableMySQL(self, TableName, Columns, ColumnsDataType):

        string = "CREATE TABLE " + TableName + " ("
        
        i = 0
        while i < len(Columns):
            if(i == len(Columns) -1):
                string = string + "`"+Columns[i]+"`" +" "+ ColumnsDataType[i] +")"
            else:
                string = string + "`"+Columns[i]+"`" +" "+ ColumnsDataType[i] +", "
            i +=1
        sql = string
        try:
            connection = self.engine.raw_connection()
            cursor = connection.cursor()
            cursor.execute(sql)
            cursor.close()
            connection.commit()
        finally:
            connection.close()
