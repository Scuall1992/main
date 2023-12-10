import re
import os

FOLDER = "cases"

import json
import pandas as pd
from dataclasses import dataclass

def subtract_df(df_original, df_subset):
    # Слияние двух DataFrame с помощью внешнего слияния (outer join) и индикатора
    merged_df = pd.merge(df_original, df_subset, how='outer', indicator=True)

    # Выбираем только те строки, которые присутствуют в оригинальном DataFrame
    df_result = merged_df[merged_df['_merge'] == 'left_only'].drop(columns=['_merge'])

    # Если вы хотите сохранить только столбцы оригинального DataFrame
    df_result = df_result[df_original.columns]

    return df_result



def parse_conditions_to_code(conditions):
    python_code = conditions.replace("AND", "&").replace("OR", "|").replace("NOT", "~")

    python_code = python_code.replace('"Площадка"', 'df_orig["Площадка"]')
    python_code = python_code.replace('"Тип используемых прав" ', 'df_orig["Тип используемых прав"]')
    python_code = python_code.replace('"Территория"', 'df_orig["Территория"]')
    python_code = python_code.replace('"Тип контента"', 'df_orig["Тип контента"]')
    python_code = python_code.replace('"Вид использования  контента"', 'df_orig["Вид использования  контента"]')
    python_code = python_code.replace('"Исполнитель"', 'df_orig["Исполнитель"]')
    python_code = python_code.replace('"Название трека"', 'df_orig["Название трека"]')
    python_code = python_code.replace('"Название альбома"', 'df_orig["Название альбома"]')
    python_code = python_code.replace('"Автор слов"', 'df_orig["Автор слов"]')
    python_code = python_code.replace('"Автор музыки"', 'df_orig["Автор музыки"]')
    python_code = python_code.replace('"ISRC контента"', 'df_orig["ISRC контента"]')
    python_code = python_code.replace('"UPC альбома"', 'df_orig["UPC альбома"]')
    python_code = python_code.replace('"Копирайт"', 'df_orig["Копирайт"]')

    python_code = re.sub(r'\s+', ' ', python_code).strip()

    python_code = f'df_orig[{python_code}]'

    return python_code


def calc_sum(df):
    res = 0
    for index, row in df.iterrows():
        res += (row.iloc[c["SUM_1"]] + row.iloc[c["SUM_2"]])
    return round(res, 2)


@dataclass
class Data:
    per: float
    name: str

def parse_filename(name: str) -> Data:
    name = name.replace(".txt", "")
    d = name.split(",")

    per = float(d[0].replace("per=", ""))
    name = d[1].replace("name=", "")

    return Data(per=per, name=name)

with open("config.json", "r", encoding="utf-8") as f:
    c = json.loads(f.read())

def run(case):
    df_orig = pd.read_excel(c["filename"], header=c["header_index"])

    col_len = len(df_orig.columns)
    if  col_len > c["col_len"]:
        df_orig = df_orig.iloc[:, :c["col_len"]-col_len]

    all_case_dfs = list()
    case_dfs = dict()

    case_path = os.path.join(FOLDER, case)
    res = 0
    
    case_dfs[case] = list()
    
    if os.path.isfile(case_path):
        with open(case_path, "r", encoding="utf-8") as f:
            conditions = f.read()
        code_output = parse_conditions_to_code(conditions)
        data = parse_filename(case)
        df = eval(code_output)

        all_case_dfs.append(df)
        case_dfs[case].append(df)

        res += calc_sum(df) * (data.per/100)
        print(round(res, 2), data.name)
    else:
        for subcase in os.listdir(case_path):
            r = 0
            with open(os.path.join(case_path, subcase), "r", encoding="utf-8") as f:
                conditions = f.read()
            code_output = parse_conditions_to_code(conditions)
            df = eval(code_output)

            all_case_dfs.append(df)
            case_dfs[case].append(df)

            data = parse_filename(subcase)
            r = calc_sum(df) * (data.per/100)

            res += r
        data.name = case

        print(round(res, 2), data.name)

    #TODO сохранить всё в файл
    #TODO если стоит галка, то сохранить оставшиеся строки


# df = df_orig[df_orig["Копирайт"] == "Pancher Label / SOEXC3LLENT"]

# sub_df = subtract_df(df, pd.concat(case_dfs["Мозговой"]).drop_duplicates())

# print(calc_sum(sub_df) * (70/100))

