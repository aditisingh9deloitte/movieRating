from crypt import methods
import boto3
from flask import Flask,request,jsonify
import movie_service as dynamodb
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
    data = []

    if request.files:
        # print('1')
        uploaded_file = request.files['file']
        # print('data', request.files['file'])#, uploaded_file.filename, allowed_file(uploaded_file.filename))
        if uploaded_file.filename and allowed_file(uploaded_file.filename):
            # print('2')
            filepath = os.path.join(directory_path, uploaded_file.filename)
            # print('3')
            uploaded_file.save(filepath)
            # print('4')
        
            with open(filepath) as file:
                # print('yes')
                csv_file = csv.DictReader(file)
                for row in csv_file:
                    # print('*****data', len(row['year']))
                    dynamodb.write_movie_info(row)
                    data.append(row)
            return jsonify({
                'responseType' : 'success',
                'status'       : 201,
                'message'      : 'created successfully',
                'data' : data 
            },201)    

        else:
            return jsonify({
                    'responseType' : 'Invalid file format',
                    'status' : 500,
                    'message' : 'Please upload a vald csv file'
                }), 500
 
@app.route('/movie_directed_in_a_particular_timerange', methods=['GET'])
def movies_directed_in_year_range():

    director = request.args.get("director")
    yearFrom = request.args.get("yearFrom")
    yearTo = request.args.get("yearTo")

    
    if(director=='' or yearFrom=='' or yearTo=='' or int(yearTo)<int(yearFrom)):
        return jsonify({
                'responseType' : 'failure',
                'status' : 500,
                'message' : 'Please provide director, and valid year range'
            }), 500

    else:

        data = dynamodb.get_movie_info_wrt_director(director, int(yearFrom), int(yearTo))

        if(len(data) == 0):
            print('yes')
            return jsonify({
                'responseType' : 'No Data Available',
                'data' : data,
                'status' : 204
            }), 200

        return jsonify({
                'responseType' : 'success',
                'data' : data,
                'status' : 200
            }), 200

@app.route('/filter_acc_to_user_review', methods=['GET'])
def filteration_wrt_given_user_review():

    user_review = request.args.get("user_review")

    try:
        user_review = int(user_review)
    except Exception:
        return jsonify({
            'responseType' : 'failure',
            'status'       : 500,
            'message'      : 'Please user-review a number'
        }), 500

    data = dynamodb.get_movies_greater_than_given_user_review(user_review)

    return  jsonify({
        'data'         : data,
        'responseType' : 'success',
        'status'       : 200,
        'message'      : 'Data retreived successfully'
        }), 200


@app.route('/filter_highest_budget_movies', methods=['GET'])
def highest_budget_movies():

    country = request.args.get("country")
    year = request.args.get("year")


    try:
        year = int(year)
        if country == '':
            raise ValueError('invalid country')

    except Exception:
        return jsonify({
            'responseType' : 'failure',
            'status'       : 500,
            'message'      : 'Please provide with a valid year and country'
        }), 500

    data = dynamodb.get_highest_budget_movies(country, year)

    if len(data) == 0:
        return  jsonify({
        'data'         : data,
        'responseType' : 'success',
        'status'       : 204,
        'message'      : 'No records'
        }), 200

    return  jsonify({
        'data'         : data[0],
        'responseType' : 'success',
        'status'       : 200,
        'message'      : 'Data retreived successfully'
        }), 200

@app.route('/movie/<int:id>', methods=['DELETE'])
def delete_movie_info(id):
    response = dynamodb.delete_movie_information(id)
    if (response['ResponseMetadata']['HTTPStatusCode'] == 200):
        return {
            'msg': 'Deleted successfully',
        }
    return {  
        'msg': 'Some error occcured',
        'response': response
    } 























































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