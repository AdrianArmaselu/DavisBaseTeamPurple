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
def parseUserCommand(commandTokens):
    commandType = commandTokens[0]
    global isExit
    # DML Cases
    if commandType == SELECT:
        print("select path")
    elif commandType == UPDATE:
        print("update path")
    elif commandType == INSERT:
        print("insert path")
    elif commandType == DELETE:
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
        commandTokens = input(prompt).strip().lower().split(" ")
        parseUserCommand(commandTokens)
    print("\nExiting...")


if __name__ == "__main__":
    main()
