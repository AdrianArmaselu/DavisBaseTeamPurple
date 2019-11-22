import re

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


# Method to display the splash screen
def splashScreen():
    print("-" * 80)
    print("Welcome to Davisbase")
    print("DavisBaseLite Version " + version)
    print("\nType \"help;\" to display supported commands.")
    print("-" * 80)


# Method to parse table name, list of columns and values to be inserted for those columns
def parseInsert(commandTokens):
    columnList = commandTokens[3].split(",")
    tableName = commandTokens[4]
    valueList = commandTokens[-1].split(",")
    columnValueMap = {}
    for i in range(len(columnList)):
        columnValueMap[columnList[i]] = valueList[i]
    insertHandler(tableName, columnValueMap)


# Stub method to handle the actions of insert based on table name and column value mapping
def insertHandler(tableName, columnValueMap):
    print("Table name: " + tableName)
    print("Column value dictionary: " + str(columnValueMap))


# Method to parse table name, condition1, operator and condition2
def parseDelete(commandTokens):
    condition1 = None
    operator = None
    condition2 = None
    tableName = commandTokens[3]
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
    updateHandler(updateValuesDictionary, tableName, condition1, operator, condition2)


# Stub method to perform update action. Data to be updated is stored as key / value pairs
# Key refers to the column name and value refer to the updated value for that particular column
def updateHandler(updateValuesDictionary, tableName, condition1, operator, condition2):
    print("Column names: " + str(updateValuesDictionary))
    print("Table names: " + tableName)
    print("Condition 1: " + condition1)
    print("Operator: " + operator)
    print("Condition 2: " + condition2)


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
    selectHandler(columnNames, tableName, condition1, operator, condition2)


# Stub method to perform action based on select command
# Write your select action here.
def selectHandler(columnNames, tableName, condition1=None, operator=None, condition2=None):
    print("Column names: " + str(columnNames))
    print("Table names: " + tableName)
    if condition1 and condition2 and operator:
        print("Condition 1: " + condition1)
        print("Operator: " + operator)
        print("Condition 2: " + condition2)


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
        print("insert path")
    elif commandType == DELETE:
        commandTokens = queryString.replace(";", "").split(" ")
        parseDelete(commandTokens)
        print("delete path")

    # DDL Cases
    elif commandType == CREATE:
        print("create path")
    elif commandType == DROP:
        print("drop path")
    elif commandType == SHOW:
        print("show path")

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


if __name__ == "__main__":
    main()
