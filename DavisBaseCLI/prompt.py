prompt = "davisql> "
version = "v1.0"
isExit = False


# Method to display the splash screen
def splashScreen():
    print("-" * 80)
    print("Welcome to Davisbase")
    print("DavisBaseLite Version " + version)
    print("\nType \"davisHelp;\" to display supported commands.")
    print("-" * 80)


# Method to display commands supported in Davisbase
def davisHelp():
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
    print("\tDisplay this davisHelp information.\n")
    print("EXIT")
    print("\tExit the program.\n")
    print("*" * 80)


# Method to accept user command and determine the command type
def parseUserCommand(commandTokens):
    commandType = commandTokens[0]
    global isExit
    # DML Cases
    if commandType == "select":
        print("select path")
    elif commandType == "update":
        print("update path")
    elif commandType == "insert":
        print("insert path")
    elif commandType == "delete":
        print("delete path")

    # DDL Cases
    elif commandType == "create":
        print("create path")
    elif commandType == "drop":
        print("drop path")
    elif commandType == "show":
        print("show path")

    # Miscellaneous commands'
    elif commandType == "davisHelp":
        davisHelp()
    elif commandType == "quit" or commandType == "exit":
        isExit = True

    # Invalid query
    else:
        print("Invalid query")


# Entry point of application. Runs until exit or quit command is entered.
def main():
    splashScreen()
    while not isExit:
        commandTokens = input(prompt).strip().lower().split(" ")
        parseUserCommand(commandTokens)
    print("\nExiting...")


if __name__ == "__main__":
    main()
