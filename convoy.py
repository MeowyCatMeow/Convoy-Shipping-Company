import pandas as pd
import csv
import re
import sqlite3


def xlsx_to_csv(input_file_name):
    my_df = pd.read_excel(input_file_name, sheet_name='Vehicles', dtype=str)
    csv_file_name = input_file_name.replace('.xlsx', '.csv')
    my_df.to_csv(csv_file_name, index=False)
    line_num = my_df.shape[0]
    print(f'{line_num} line{"s were" if line_num > 1 else " was"} imported to {csv_file_name}')
    return csv_file_name


def clean_csv(csv_file_name):
    csv_checked_name = csv_file_name[:-4] + '[CHECKED].csv'
    checked_file = open(csv_checked_name, 'w')
    with open(csv_file_name, 'r') as csv_file:
        file_reader = csv.reader(csv_file, delimiter=",")
        file_writer = csv.writer(checked_file, delimiter=",", lineterminator="\n")
        count = 0
        wrong_data_count = 0
        for line in file_reader:
            if count == 0:
                file_writer.writerow(line)
            else:
                for cell in line:
                    if not cell.isnumeric():
                        wrong_data_count += 1
                        line[line.index(cell)] = int(re.findall(r'\d+', cell)[0])
                file_writer.writerow(line)
            count += 1
    checked_file.close()
    if wrong_data_count > 0:
        print(f'{wrong_data_count} cell{"s were" if wrong_data_count > 1 else " was"} corrected in {csv_checked_name}')
    return csv_checked_name


def csv_to_s3db(cleaned_csv):
    database = cleaned_csv.removesuffix('.csv') + '.s3db'
    database = database.replace('[CHECKED]', '')
    conn = sqlite3.connect(database)
    cursor_name = conn.cursor()
    with open(cleaned_csv, 'r') as csv_file:
        file_reader = csv.reader(csv_file, delimiter=",")
        count = 0
        for line in file_reader:
            if count == 0:
                cursor_name.execute('''
                    CREATE TABLE IF NOT EXISTS convoy (
                    [vehicle_id] INT PRIMARY KEY,
                    [engine_capacity] INT NOT NULL,
                    [fuel_consumption] INT NOT NULL,
                    [maximum_load] INT NOT NULL,
                    [score] INT NOT NULL  
                    );
                    ''')
            else:
                score = score_calculator(line)
                line.append(str(score))
                cursor_name.execute('''
                INSERT INTO convoy (vehicle_id, engine_capacity, fuel_consumption, maximum_load, score)
                VALUES (?,?,?,?,?);
                ''', line)
            count += 1
    count -= 1 if count > 0 else 0
    conn.commit()
    conn.close()
    print(f'{count} record{"s were" if count > 1 else " was"} inserted into {database}')
    return database


def score_calculator(line):
    line = [int(x) for x in line]
    vehicle_id, engine_capacity, fuel_consumption, maximum_load = line
    score = 0
    stops = engine_capacity / fuel_consumption
    if stops > 4.5:
        score += 2
    elif stops * 2 <= 4.5:
        score += 0
    else:
        score += 1
    score += 1 if 450 / 100 * fuel_consumption > 230 else 2
    score += 0 if maximum_load < 20 else 2
    return score


def s3db_to_json_xml(input_file_name):
    json_name = input_file_name.replace('s3db', 'json')
    xml_name = input_file_name.replace('s3db', 'xml')
    conn = sqlite3.connect(input_file_name)
    df = pd.read_sql('SELECT * FROM convoy', conn)
    conn.close()
    df_json = df[df['score'] > 3]
    df_json.pop('score')
    df_xml = df[df['score'] <= 3]
    df_xml.pop('score')
    result = df_json.to_json(orient='records')
    json_data = '{"convoy":' + result + '}'
    xml_data = df_xml.to_xml(root_name='convoy', row_name='vehicle', xml_declaration=False, index=False)
    with open(json_name, 'w') as json_file:
        json_file.write(json_data)
    print(f'{len(df_json.index)} vehicle{"s were" if len(df.index) > 1 else " was"} saved into {json_name}')
    with open(xml_name, 'w') as xml_file:
        if len(df_xml.index) > 0:
            xml_file.write(xml_data)
        else:
            xml_file.write('<convoy></convoy>')
    print(f'{len(df_xml.index)} vehicles were saved into {xml_name}')


def main():
    input_file_name = input('Input file name\n')
    if input_file_name.endswith('.xlsx'):
        csv_file_name = xlsx_to_csv(input_file_name)
        checked_name = clean_csv(csv_file_name)
        database = csv_to_s3db(checked_name)
        s3db_to_json_xml(database)
    elif input_file_name.endswith('.csv'):
        if '[CHECKED]' not in input_file_name:
            checked_name = clean_csv(input_file_name)
        else:
            checked_name = input_file_name
        database = csv_to_s3db(checked_name)
        s3db_to_json_xml(database)
    elif input_file_name.endswith('s3db'):
        s3db_to_json_xml(input_file_name)


if __name__ == '__main__':
    main()
