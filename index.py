import pandas 
import xml.etree.ElementTree as ET
import json


source_1 = "data_Soruce_1.xml"
source_2 = "data_Source_2.xml"
source_3 = "data_Source_3.json"
result_first_part = []
result_second_part = []


# Парсим первый файл
def get_data_from_source_1():
    data_Source_1 = ET.parse(source_1)
    parfum = {'ean':[], 'id':[], 'name_source_1':[], 'brand':[], 'line':[]}
    for element in data_Source_1.findall("SHOPITEM"):
        try:
            parfum['ean'].append(element.find("EAN").text)
            parfum['id'].append(element.find("id").text)
            parfum['name_source_1'].append(element.find("NAME").text)
            parfum['brand'].append(element.find("MANUFACTURER").text.lower())
            parfum['line'].append(element.find("RANGE").text.lower())
        except AttributeError:
            continue
    return parfum

        
# парсим второй файл   
def get_data_from_source_2():
    parfum = {'ean':[], 'id':[], 'name_source_2':[], 'brand':[], 'line':[]}
    data_Source_2 = ET.parse(source_2)
    for element in data_Source_2.findall("Product"):
        parfum['ean'].append(element.find("EAN").text if element.find("EAN").text else 'none')
        parfum['id'].append(element.find("id").text)
        parfum['name_source_2'].append(element.find("Description").text)
        parfum['brand'].append(element.find("Brand").text.lower() if element.find("Brand").text else 'none')
        parfum['line'].append(element.find("BrandLine").text.lower() if element.find("BrandLine").text else 'none')
    return parfum


# парсим третий файл
def get_data_from_source_3 ():
    parfum = {'ean':[], 'id':[], 'name_source_3':[], 'brand':[], 'line':[]}
    with open(source_3, 'r', encoding="UTF-8") as file:
        data = file.read()
        data_Source_3 = json.loads(data, strict=False)
    for element in data_Source_3:
        parfum['ean'].append(element["EANs"][0])
        parfum['id'].append(element["LineaId"])
        parfum['name_source_3'].append(element["name"])
        parfum['brand'].append(element["BrandName"].lower())
        parfum['line'].append(element["LineaName"].lower())
    return parfum   


# Для каждой базы данных создаём объект DataFrame Pandas
db1 = pandas.DataFrame(get_data_from_source_1())
db2 = pandas.DataFrame(get_data_from_source_2())
db3 = pandas.DataFrame(get_data_from_source_3())


# Совмещаем базы данных по общему ean
merge_ean_1 = pandas.merge(db1, db2, how="inner", on=['ean'])
merge_ean_result = pandas.merge(merge_ean_1, db3,  how="inner", on=['ean'])


# Совмещаем базы данных по общему брэнду и линии и убираем одинаковые
merge_name_1 = pandas.merge(db1, db2, how="inner", on=['brand', 'line'])
merge_name_2 = pandas.merge(merge_name_1, db3, how="inner", on=['brand', 'line'])
merge_name_result = merge_name_2.drop_duplicates(subset='id')


# формируем список товаров, сортированных по имени
for item in merge_name_result.iterrows():
    product = [{
        "source":"source_1",
        "name":item[1]['name_source_1'],
        "ean_code":item[1]['ean_x'],
        "id":item[1]['id_x']
    },
    {
        "source":"source_2",
        "name":item[1]['name_source_2'],
        "ean_code":item[1]['ean_y'],
        "id":item[1]['id_y']
    },
    {
        "source":"source_3",
        "name":item[1]['name_source_3'],
        "ean_code":item[1]['ean'],
        "id":item[1]['id']
    }
    ]
    result_second_part.append(product)


# формируем список товаров, сортированных по ean
for item in merge_ean_result.iterrows():
    product = [{
        "source":"source_1",
        "name":item[1]['name_source_1'],
        "ean_code":item[1]['ean'],
        "id":item[1]['id_x']
    },
    {
        "source":"source_2",
        "name":item[1]['name_source_2'],
        "ean_code":item[1]['ean'],
        "id":item[1]['id_y']
    },
    {
        "source":"source_3",
        "name":item[1]['name_source_3'],
        "ean_code":item[1]['ean'],
        "id":item[1]['id']
    }
    ]
    result_first_part.append(product)


result = {"sort_by_ean":result_first_part, 
            "sort_by_name":result_second_part}


# записываем данные в файл newResult.json
with open('newResult.json', 'w') as file:
    json.dump(result, file, indent=4)



