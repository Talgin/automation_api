"""
Please, insert this line in any code you use the following code

Developed by: Islamgozhayev Talgat and Akbergenov Yerkin

Explanation: API is written to solve the problem of updating

regional server database with new information from central server.
"""
from typing import Optional, List
from fastapi import FastAPI, File, UploadFile, Form, Response, status
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
# Importing miscellaneous modules
import os
from datetime import datetime
import configs
import time
import pickle
import json
import ast
import faiss as fs
from db.faisser import Faisser
import numpy as np
# Akati modules
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import VARCHAR, DATE

app = FastAPI()

db_worker = Faisser(configs.LOCAL_FAISS_BACKUP_PATH)

@app.post("/check_regional_tar", status_code=200)
async def check_regional_tar(response: Response, folder_name: str = Form(...), tar_size: str = Form(...)):
    message = None
    print(folder_name)
    as_list = ast.literal_eval(tar_size)
    tar_size = as_list[0]['tar_size']
    print(tar_size)
    full_path = os.path.join(configs.LOCAL_DATA_ROOT, folder_name)
    if os.path.exists(full_path):
        tar_path = os.path.join(full_path, folder_name + '.tar.gz')
        if not str(tar_size) == str(os.stat(tar_path).st_size):
            response.status_code = status.HTTP_404_NOT_FOUND
            return {'status': 'error', 'original-regional': str(tar_size) + ' - ' + str(os.stat(tar_path).st_size), 'message': 'Sizes mismatch'}
        else:
            response.status_code = status.HTTP_200_OK
            return {'status': 'success', 'original-regional': str(tar_size) + ' - ' + str(os.stat(tar_path).st_size), 'message': 'OK'}
    else:
        response.status_code = status.HTTP_404_NOT_FOUND
        return {'status': 'error', 'message': 'File(s) not found'}


@app.post("/check_regional_data", status_code=200)
async def check_regional_data(response: Response, folder_name: str = Form(...), files_size: str = Form(...)):
    message = None

    print(files_size)
    full_path = os.path.join(configs.LOCAL_DATA_ROOT, folder_name)
    if os.path.exists(full_path):
        print("I'm in folder:", full_path)
        if len(os.listdir(os.path.join(full_path))) > 1:
            photo_folder = os.path.join(full_path, 'photo')
            json_folder = os.path.join(full_path, 'jsons')
            pickle_folder = os.path.join(full_path, 'pickles')
            as_list = ast.literal_eval(files_size)
            file_names_sizes = as_list[0]['files_sizes']
            error_files = {}
            if len(os.listdir(photo_folder)) > 0:
                for photo in os.listdir(photo_folder):
                    print(photo_folder + '/' + photo)
                    if not str(file_names_sizes[photo]) == os.stat(photo_folder + '/' + photo).st_size:
                        error_files[photo] = photo
                for json_file in os.listdir(json_folder):
                    print(json_folder + '/' + json_file)
                    if not str(file_names_sizes[json_file]) == os.stat(json_folder + '/' + json_file).st_size:
                        error_files[json_file] = json_file
                for pickle_file in os.listdir(pickle_folder):
                    print(pickle_folder + '/' + pickle_file)
                    if not str(file_names_sizes[pickle_file]) == os.stat(pickle_folder + '/' + pickle_file).st_size:
                        error_files[pickle_file] = pickle_file
                # file_names_sizes['tar_file'] = os.stat.st_size(full_path + '/' + todays_folder+'.tar.gz')

                response.status_code = status.HTTP_200_OK
                return {'status': 'success', 'errors': len(error_files), 'error_files': error_files}
    else:
        response.status_code = status.HTTP_404_NOT_FOUND
        return {'status': 'error'}


@app.post("/update_faiss", status_code=200)
async def update_faiss(response: Response, folder_name: str = Form(...)):
    message = None
    print(folder_name)
    pickle_path = os.path.join(configs.LOCAL_DATA_ROOT, folder_name, 'pickles/to_update.pickle')
    print(pickle_path)
    if os.path.exists(pickle_path):
        data = None
        with open(pickle_path,"rb") as f:
            try:
                data = pickle.load(f)
            except EOFError:
                pass

        before = db_worker.get_records_amount()
        for i in data.keys():
            print("Amount in current faiss:", db_worker.get_records_amount())
            message = db_worker.insert_person_into_faiss(i, data[i])
            if message['status'] == 'success':
                print("Amount after insertion:", db_worker.get_records_amount())
            else:
                print(message)
        after = db_worker.get_records_amount()
        response.status_code = status.HTTP_200_OK
        return {'status': 'success', 'before': before, 'after': after}
    else:
        response.status_code = status.HTTP_404_NOT_FOUND
        return {'status': 'error', 'message': 'faiss index not found in ' + pickle_path}

@app.post("/update_faiss_backup", status_code=200)
async def update_faiss_backup(response: Response, folder_name: str = Form(...)):
    message = None
    print(folder_name)
    pickle_path = os.path.join(configs.LOCAL_DATA_ROOT, folder_name, 'pickles/to_update.pickle')
    print(pickle_path)
    if os.path.exists(pickle_path):
        data = None
        with open(pickle_path,"rb") as f:
            try:
                data = pickle.load(f)
            except EOFError:
                pass

        before = db_worker.get_records_amount()
        for i in data.keys():
            print("Amount in CURRENT faiss:", db_worker.get_records_amount())
            result = db_worker.insert_into_faiss(i, data[i])
            if result:
                print("Amount AFTER insertion:", db_worker.get_records_amount())
            else:
                print("Could not insert:", result)
        after = db_worker.get_records_amount()
        save_index = db_worker.save_faiss_index(configs.LOCAL_FAISS_BACKUP_PATH)
        if after > before:
            response.status_code = status.HTTP_200_OK
            return {'status': 'success', 'before': before, 'after': after}
        else:
            response.status_code = status.HTTP_404_NOT_FOUND
            return {'status': 'error', 'message': 'faiss index not found in ' + pickle_path}
    else:
        response.status_code = status.HTTP_404_NOT_FOUND
        return {'status': 'error', 'message': 'Could not save faiss ' + pickle_path}

@app.post("/update_temporary_tables", status_code=200)
async def update_temporary_tables(response: Response, folder_name: str = Form(...)):
    jsons_path = os.path.join(configs.LOCAL_DATA_ROOT, folder_name, 'jsons')
    print(jsons_path)
    if os.path.exists(jsons_path):
        # Establishing database connection
        conn_string = 'postgresql://'+configs.PG_USER+':'+configs.PG_PASS+'@'+configs.PG_HOST+':'+configs.PG_PORT+'/'+configs.PG_DATABASE
        engine = create_engine(conn_string)
        con = engine.connect()
        # Reading jsons as dataframes
        df1 = pd.read_json(os.path.join(jsons_path, 'df_died.json'), dtype = {'PERSON_IIN': str})

        df2 = pd.read_json(os.path.join(jsons_path, 'df_delete.json'), dtype = {'GRCODE': str, 'UDCODE': str})

        df3 = pd.read_json(os.path.join(jsons_path, 'df_insert.json'), dtype = {'GRCODE': str, 'UDCODE': str})

        # Inserting df-s into temporary tables
        df1.to_sql('died_tmp',con, schema='fr', if_exists = 'replace', index= False, dtype = {"PERSON_IIN": VARCHAR(20)})

        df2.to_sql('delete_tmp',con, schema='fr', if_exists = 'replace', index= False, dtype = {"GRCODE": VARCHAR(20), "UDCODE": VARCHAR(20)})

        df3.to_sql('insert_tmp',con, schema='fr', if_exists = 'replace', index= False, dtype = {"GRCODE": VARCHAR(20), "UDCODE": VARCHAR(20), "LASTNAME": VARCHAR(40), "FIRSTNAME": VARCHAR(40), "SECONDNAME": VARCHAR(40), "UDDATE": DATE})

        response.status_code = status.HTTP_200_OK
        return {'status': 'success'}
    else:
        response.status_code = status.HTTP_404_NOT_FOUND
        return {'status': 'error', 'message': 'JSON folder not found in ' + jsons_path}

@app.post("/faiss_create_new_index", status_code=200)
async def faiss_create_new_index(response: Response, folder_name: str = Form(...)):
    message = None
    print(folder_name)
    pickle_path = os.path.join(configs.LOCAL_DATA_ROOT, folder_name, 'pickles/to_update.pickle')
    print(pickle_path)
    if os.path.exists(pickle_path):
        data = None
        identificators = []
        vectors = []
        with open(pickle_path,"rb") as f:
            try:
                data = pickle.load(f)
            except EOFError:
                return {'status': 'error', 'message': 'pickle not found in ' + pickle_path}
        for k in data.keys():
            identificators.append(k)
            vectors.append(data[k])

        # Formatting vectors and ids
        new_vectors = np.array(vectors, dtype=np.float32)
        new_ids_np = np.array(list(map(int, identificators)))
        print('Data converted')

        try:
            result = db_worker.create_block_and_index(new_vectors, new_ids_np, configs.TRAINED_INDEX_PATH, 
                                                    configs.PATH_TO_SAVE_NEW_BLOCK, configs.MERGED_INDEX_PATH, 
                                                    configs.LOCAL_FAISS_BACKUP_PATH)
        except:
            result = False
        if result['status'] == True:
            return {'status': 'success', 'index-size': result['size']}
        else:
            return {'status': 'error', 'message': 'Could not save new faiss ' + configs.LOCAL_FAISS_BACKUP_PATH}
    else:
        response.status_code = status.HTTP_404_NOT_FOUND
        return {'status': 'error', 'message': 'Could not save new faiss ' + configs.LOCAL_FAISS_BACKUP_PATH}
