import csv
from Schema.Schema import RegionInfo, Case, Region, PatientInfo, DateHistory
from mongoengine import connect, disconnect
import pathlib
from datetime import datetime


def read_csv(filename, delimiter):
    csv_file = open(filename, 'r')
    reader = csv.DictReader(csv_file, delimiter=delimiter)

    return list((dict(row) for row in reader))


def create_collection(name_db, db):
    connect(name_db)
    base_path = pathlib.Path().absolute()
    dict_case = read_csv(str(base_path) + "/COVID19CSV/Case.csv", ',')
    dict_region = read_csv(str(base_path) + "/COVID19CSV/Region.csv", ';')
    dict_patient = read_csv(str(base_path) + "/COVID19CSV/PatientInfo.csv", ';')

    for row in dict_region:
        RegionInfo(code=row['code'],
                   province=row['province'],
                   city=row['city'],
                   elementary_school_count=row['elementary_school_count'],
                   kindergarten_count=row['kindergarten_count'],
                   university_count=row['university_count'],
                   nursing_home_count=row['nursing_home_count'],
                   academy_ratio=float(row['academy_ratio']),
                   elderly_population_ratio=float(row['elderly_population_ratio']),
                   elderly_alone_ratio=float(row['elderly_alone_ratio'])).save()

    for row in dict_patient:
        region_info = db.region_info.find_one({'city': row['city']}, {'_id': 1,
                                                                      'nursing_home_count': 0,
                                                                      'academy_ratio': 0,
                                                                      'elderly_population_ratio': 0,
                                                                      'elderly_alone_ratio': 0})
        if row['city'] == 'etc' or row['city'] == '' or region_info is None:
            r_info = None
        else:
            region_info.pop('_id')
            r_info = Region(**region_info)

        date_history = {
            'symptom_onset_date': (
                datetime.strptime(row['symptom_onset_date'], '%d/%m/%y') if row['symptom_onset_date'] != '' and row[
                    'symptom_onset_date'] != ' ' else None),
            'confirmed_date': (
                datetime.strptime(row['confirmed_date'], '%d/%m/%y') if row['confirmed_date'] != '' and row[
                    'confirmed_date'] != ' ' else None),
            'released_date': (
                datetime.strptime(row['released_date'], '%d/%m/%y') if row['released_date'] != '' and row[
                    'released_date'] != ' ' else None),
            'deceased_date': (
                datetime.strptime(row['deceased_date'], '%d/%m/%y') if row['deceased_date'] != '' and row[
                    'deceased_date'] != ' ' else None),
        }
        d_history = DateHistory(**date_history)
        info = {'patient_id': row['patient_id'], 'sex': row['sex'],
                'age': None if row['age'] == "" else int(row['age']),
                'region': r_info,
                'infection_case': None if row['infection_case'] == "" else row['infection_case'],
                'infected_by': None if row['infected_by'] == "" else row['infected_by'], 'date_history': d_history,
                'state': row['state']}
        PatientInfo(**info).save()

    for row in dict_case:
        region_info = db.region_info.find_one({'city': row['city']})
        # print(region_info)
        r_info = dict()
        if row['city'] == 'etc' or row['city'] == '' or region_info is None:
            r_info = None
        else:
            r_info['_id'] = region_info.pop('_id')
            r_info['city'] = region_info.pop('city')

        info = {'case_id': row['case_id'], 'group': True if row['group'] == 'TRUE' else False,
                'confirmed': row['confirmed'], 'infection_case': row['infection_case'], 'region': r_info}
        Case(**info).save()

    disconnect()
