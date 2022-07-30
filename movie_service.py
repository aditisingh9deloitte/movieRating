from boto3 import resource
import config

AWS_ACCESS_KEY_ID = config.AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY = config.AWS_SECRET_ACCESS_KEY
REGION_NAME = config.REGION_NAME
ENDPOINT_URL = config.ENDPOINT_URL

resource = resource(
    'dynamodb',
    endpoint_url          = ENDPOINT_URL,
    aws_access_key_id     = AWS_ACCESS_KEY_ID,
    aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
    region_name           = REGION_NAME
)

def create_table_movie():    
    table = resource.create_table(
        TableName = 'Movie', #Name of the table
        KeySchema = [
            {
                'AttributeName': 'id',
                'KeyType'      : 'HASH' # HASH = partition key, RANGE = sort key
            }
        ],
        AttributeDefinitions = [
            {
                'AttributeName': 'id', # Name of the attribute
                'AttributeType': 'N'   # N = Number (S = String, B= Binary)
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits'  : 20,
            'WriteCapacityUnits': 20
        }
    )
    return table

movieTable = resource.Table('Movie')

def write_movie_info(data):
    response = movieTable.put_item(
        Item = {
            'id'          :  int(data['imdb_title_id'].split('tt')[1]),
            'imdb_title_id'          :  data['imdb_title_id'], 
            'title'                  :  data['title'], 
            'original_title'         :  data['original_title'],  
            'year'                   :  data['year'], 
            'date_published'         :  data['date_published'], 
            'genre'                  :  data['genre'], 
            'duration'               :  data['duration'], 
            'country'                :  data['country'], 
            'language'               :  data['language'], 
            'director'               :  data['director'], 
            'writer'                 :  data['writer'], 
            'production_company'     :  data['production_company'], 
            'actors'                 :  data['actors'],  
            'description'            :  data['description'], 
            'avg_vote'               :  data['avg_vote'],
            'votes'                  :  data['votes'],
            'budget'                 :  data['budget'],
            'usa_gross_income'       :  data['usa_gross_income'],
            'worlwide_gross_income'  :  data['worlwide_gross_income'],
            'metascore'              :  data['metascore'], 
            'reviews_from_users'     :  data['reviews_from_users'], 
            'reviews_from_critics'   :  data['reviews_from_critics']
        }
    )
    return response

