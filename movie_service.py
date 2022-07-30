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


def write_movie_info(imdb_title_id, title, original_title, year, date_published, 
                    genre, duration, country, language, director, 
                    writer, production_company, actors, description, avg_vote, votes, budget, 
                    usa_gross_income, worlwide_gross_income, metascore, reviews_from_users, reviews_from_critics):
    response = movieTable.put_item(
        Item = {
            'imdb_title_id'          :  imdb_title_id, 
            'title'                  :  title, 
            'original_title'         :  original_title,  
            'year'                   :  year, 
            'date_published'         :  date_published, 
            'genre'                  :  genre, 
            'duration'               :  duration, 
            'country'                :  country, 
            'language'               :  language, 
            'director'               :  director, 
            'writer'                 :  writer, 
            'production_company'     :  production_company, 
            'actors'                 :  actors,  
            'description'            :  description, 
            'avg_vote'               :  avg_vote,
            'votes'                  :  votes,
            'budget'                 :  budget,
            'usa_gross_income'       :  usa_gross_income,
            'worlwide_gross_income'  :  worlwide_gross_income,
            'metascore'              :  metascore, 
            'reviews_from_users'     :  reviews_from_users, 
            'reviews_from_critics'   :  reviews_from_critics
        }
    )
    return response


