from pymongo import MongoClient
client = MongoClient('mongodb+srv://RubenFerreira:TPVXAliOZt3OqFpk@11sixteen.zzyri.mongodb.net/test?')
db = client['football_data']
collection = db['football_results']

results = collection.aggregate(
    [
        {'$group':
            {'_id':
                {
                    'start_time': '$event.date_time',
                    'home_team': '$teams.home',
                    'away_team': '$teams.away',
                    'home_score': '$results.FTHG',
                    'away_score': '$results.FTAG'
},
             'uniqueIds':   {'$addToSet': "$_id"},
             'count':       {'$sum': 1}
             }
         },

        {'$match':
            {'count':
                {'$gte': 2}
             }
         }
    ])
for result in results:
    print(f"Game: {result['_id']['home_team']} vs {result['_id']['away_team']} on {result['_id']['start_time'][:10]}")
    print(f"Final score: {result['_id']['home_score']}-{result['_id']['away_score']} \t "
          f"Number of copies: {result['count']}")
    for uniID in result['uniqueIds']:
        print(f"Document number: {uniID}")

