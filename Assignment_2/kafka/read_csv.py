from csv import reader

file_path = "D:/2021-2023_MDSE/1.1/Data Engineering/Assignments/data/Credit_card_transactions/test_2.csv"
print('\n')

with open(file_path) as f:
    lines = reader(f)
    for line in lines:
        msg = ''
        for column in line:
            column = column.replace(',','')
            msg += column + ','
        print(msg[:-1], '\n')
        