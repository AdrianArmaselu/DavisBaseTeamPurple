import re

from core.model import DavisBase, TableColumnsMetadata, data_type_encodings, SelectArgs, Condition, DeleteArgs, \
    UpdateArgs, ColumnDefinition

prompt = "davisql> "
version = "v1.0"
isExit = False
DEFAULT = "default"
NOT_NULL = "not null"
PRIMARY_KEY = "primary key"
UNIQUE = "unique"
NA = "na"
YES = "yes"
ERROR = "Error occurred. Please check the syntax."
HISTORY = "history"
QUIT = "quit"
EXIT = "exit"
VERSION = "version"
HELP = "help"
SHOW = "show"
DELETE = "delete"
DROP = "drop"
UPDATE = "update"
INSERT = "insert"
CREATE = "create"
SELECT = "select"
TABLE = "table"
INDEX = "index"

davis_base = DavisBase()


# Method to display the splash screen
def splashScreen():
    print("-" * 80)
    print("Welcome to Davisbase")
    print("DavisBaseLite Version " + version)
    print("\nType \"help;\" to display supported commands.")
    print("-" * 80)


# Method to parse table name and column information such as its datatype, constraints from the query
def parseCreateTable(tableName, columnInformationString):
    parsedColumnInfoList = []
    for columnInfo in columnInformationString:
        parsedColumnInfoMap = {"columnName": None, "dataType": None, "isUnique": False,
                               "isNotNull": False, "isPrimaryKey": False}
        if "unique" in columnInfo:
            parsedColumnInfoMap["isUnique"] = True
        if "primary" in columnInfo:
            parsedColumnInfoMap["isPrimaryKey"] = True
        if "not null" in columnInfo:
            parsedColumnInfoMap["isNotNull"] = True
        parsedColumnInfoMap["columnName"] = columnInfo.split(" ")[0]
        parsedColumnInfoMap["dataType"] = columnInfo.split(" ")[1]
        parsedColumnInfoList.append(parsedColumnInfoMap)

    metadata = {}
    index = 0;
    for column in parsedColumnInfoList:
        metadata[column['columnName']] = ColumnDefinition(column['dataType'].upper(),index)
        index += 1
    # print("table name", tableName)
    # print("create table metadata", metadata)
    davis_base.create_table(tableName, TableColumnsMetadata(metadata))
    # print(str(parsedColumnInfoList))


# Method to parse table name, list of columns and values to be inserted for those columns
def parseInsert(commandTokens):
    columnList = commandTokens[3].split(",")
    tableName = commandTokens[2]
    valueList = commandTokens[-1].split(",")
    insertHandler(tableName, valueList, columnList)


# Stub method to handle the actions of insert based on table name and column value mapping
def insertHandler(tableName, valueList, columnList=None):
    print("Table name: " + tableName)
    print("Value list: " + str(valueList))
    print("Column list: " + str(columnList))
    davis_base.insert(tableName, valueList, columnList)


# Method to parse table name, condition1, operator and condition2
def parseDelete(commandTokens):
    condition1 = None
    operator = None
    condition2 = None
    tableName = commandTokens[3]
    print("command tokens", commandTokens)
    if "where" in commandTokens:
        condition1 = commandTokens[-3]
        operator = commandTokens[-2]
        condition2 = commandTokens[-1]
    deleteHandler(tableName, condition1, operator, condition2)


# Stub method to perform delete action.
# Use the given tableName, condition1, operator and condition2 to identify and delete records from the table
def deleteHandler(tableName, condition1=None, operator=None, condition2=None):
    print("Table name: " + tableName)
    if condition1 and condition2 and operator:
        print("Condition 1: " + condition1)
        print("Operator: " + operator)
        print("Condition 2: " + condition2)
    davis_base.delete(tableName, condition1, operator, condition2)


# Method to parse table name, column names and values to be updated and condition1, operator and condition2
def parseUpdate(commandTokens):
    tableName = commandTokens[1]
    updateValuesDictionary = {}
    isColumnName = True
    mostRecentColumnName = ""
    for i in range(3, len(commandTokens) - 4):
        if commandTokens[i] == "=":
            continue
        if isColumnName:
            mostRecentColumnName = commandTokens[i]
        else:
            updateValuesDictionary[mostRecentColumnName] = commandTokens[i]
        isColumnName = not isColumnName
    condition1 = commandTokens[-3]
    operator = commandTokens[-2]
    condition2 = commandTokens[-1]
    updateHandler(commandTokens)


# Stub method to perform update action. Data to be updated is stored as key / value pairs
# Key refers to the column name and value refer to the updated value for that particular column
def updateHandler(commandTokens):
    print("Command Tokens" + str(commandTokens))

    # table name, value,
    # davis_base.update(commandTokens[1], UpdateArgs(0, commandTokens[5], Condition(0, commandTokens[8], commandTokens[9])))
    davis_base.update(commandTokens[1], commandTokens[3], commandTokens[5], commandTokens[7], commandTokens[8], commandTokens[9])


# Identifies column names, table name, conditions from the entered query
def parseSelect(commandTokens):
    condition1 = None
    operator = None
    condition2 = None
    columnNames = commandTokens[1].split(',')
    tableName = commandTokens[3]
    if "where" in commandTokens:
        condition1 = commandTokens[5]
        operator = commandTokens[6]
        condition2 = commandTokens[7]
    print (columnNames, tableName, condition1, operator, condition2)
    selectHandler(columnNames, tableName, condition1, operator, condition2)


# Stub method to perform action based on select command
# Write your select action here.
def selectHandler(columnNames, tableName, condition1=None, operator=None, condition2=None):
    # print("Column names: " + str(columnNames))
    # print("Table names: " + tableName)
    # if condition1 and condition2 and operator:
    #     print("Condition 1: " + condition1)
    #     print("Operator: " + operator)
    #     print("Condition 2: " + condition2)
    result = davis_base.select(tableName, columnNames, condition1, operator, condition2)
    for r in result:
        print(str([str(c) for c in r]))


# Perform actions to drop a table, given its name.
def dropTableHandler(tableToBeDropped):
    davis_base.drop_table(tableToBeDropped)

# Display all tables present in Davisbase
def showTablesHandler():
    davis_base.show_tables()


# Method to create index based on table name
def createIndexHandler(tableName):
    print("Create Index Table Name: " + tableName)


# Method to display commands supported in Davisbase
def help():
    print("*" * 80)
    print("SUPPORTED COMMANDS\n")
    print("All commands below are case insensitive\n")
    print("SHOW TABLES")
    print("\tDisplay the names of all tables.\n")
    print("SELECT * FROM <table_name>")
    print("Display all records in the table <table_name>.\n")
    print("SELECT <column_list> FROM <table_name> [WHERE <condition>]")
    print("\tDisplay table records whose optional <condition>")
    print("\tis <column_name> = <value>.\n")
    print("DROP TABLE <table_name>")
    print("\tRemove table data (i.e. all records) and its schema.\n")
    print(
        "UPDATE TABLE <table_name> SET <column_name> = <value> [WHERE <condition>]")
    print("\tModify records data whose optional <condition> is\n")
    print("VERSION")
    print("\tDisplay the program version.\n")
    print("HELP")
    print("\tDisplay this Help information.\n")
    print("EXIT")
    print("\tExit the program.\n")
    print("*" * 80)


# Method to accept user command and determine the command type
def parseUserCommand(queryString):
    commandType = queryString.split(" ")[0]
    global isExit
    # DML Cases
    if commandType == SELECT:
        print("select path")
        commandTokens = queryString.replace(", ", ",").replace(";", "").split(" ")
        parseSelect(commandTokens)
    elif commandType == UPDATE:
        print("update path")
        commandTokens = queryString.replace(",", "").replace(";", "").split(" ")
        parseUpdate(commandTokens)
    elif commandType == INSERT:
        commandTokens = queryString.replace(", ", ",").replace(";", "").replace("(", "").replace(")", "").split(" ")
        parseInsert(commandTokens)
    elif commandType == DELETE:
        commandTokens = queryString.replace(";", "").split(" ")
        parseDelete(commandTokens)
        print("delete path")

    # DDL Cases
    elif commandType == CREATE:
        createType = queryString.split(" ")[1]
        if createType == TABLE:
            tableName = queryString.replace(";", "").split(" ")[2]
            columnInformationString = re.findall('\(([^)]+)', queryString)[0].split(", ")
            parseCreateTable(tableName, columnInformationString)
        elif createType == INDEX:
            tableName = queryString.replace(";", "").split(" ")[-1]
            createIndexHandler(tableName)
        else:
            print(ERROR)
    elif commandType == DROP:
        tableToBeDropped = queryString.replace(";", "").split(" ")[-1]
        dropTableHandler(tableToBeDropped)
    elif commandType == SHOW:
        showTablesHandler()

    # Miscellaneous commands'
    elif commandType == HELP:
        help()
    elif commandType == QUIT or commandType == EXIT:
        isExit = True

    # Invalid query
    else:
        print(ERROR)


# Entry point of application. Runs until exit or quit command is entered.
def main():
    splashScreen()
    while not isExit:
        queryString = input(prompt).strip().lower()
        parseUserCommand(queryString)
    print("\nExiting...")
    davis_base.commit()


if __name__ == "__main__":
    main()
