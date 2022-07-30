from operator import index
from flask import Flask,request,jsonify

import movie_service as dynamodb
import pandas as pd
import csv,os

app = Flask(__name__)
directory_path = os.getcwd()
ALLOWED_EXTENSIONS = ['csv']

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.',1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/createTable')
def root_route():
    dynamodb.create_table_movie()
    return 'Table created'    


@app.route('/upload', methods=['POST'])
def file_upload():
    data = []; flag = 0

    if request.files:
        uploaded_file = request.files['file']
        if uploaded_file.filename and allowed_file(uploaded_file.filename):
            print('true')
            filepath = os.path.join(directory_path, uploaded_file.filename)
            uploaded_file.save(filepath)
        
            with open(filepath) as file:
                csv_file = csv.reader(file)

                for row in csv_file:
                    if flag == 0 : 
                        columns = row
                        coulmn_length = len(columns)
                        print('coulumns', columns, coulmn_length)
                        flag = 1
                    else:
                        data.append(row)
            return jsonify(data)
        else:
            return jsonify({
                    'responseType' : 'Invalid file format',
                    'status' : 500,
                    'message' : 'Please upload a vald csv file'
                }), 500























































'''
@app.route('/upload', methods=['POST'])
def file_upload():
    file = request.files['file']
    data = pd.read_csv(file)

    colums = data.columns
    length = len(colums)
    
    for i in data.index:  #rows

        if(data.loc[i,'genre'] == 'Drama'): print("yes")

        for j in range(length):  # columns
            print(data.iloc[i,j], end=' ')
        print()
        
    return {
        'msg' : 'success'
    }
'''


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000, debug=True)