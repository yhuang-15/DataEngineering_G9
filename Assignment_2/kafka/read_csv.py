from csv import reader

file_path = "test.csv"
print('\n')

with open(file_path) as f:
    lines = reader(f)
    for line in lines:
        msg = ''
        for column in line:
            column = column.replace(',','')
            msg += column + ','
        print(msg[:-1], '\n')
        