import pymongo
import json
import requests

class jsonToMongo:

    def __init__(self, database, mongoColletion, empresa, folha):
        self.database = database
        self.mongoColletion = mongoColletion
        self.folha = folha
        self.empresa = empresa

    def importTo(self):
    
        try:
            server = pymongo.MongoClient("mongodb://szarca:msabor1250@192.168.254.203:27017/?authMechanism=DEFAULT")
            db = server[self.database]
            collection = db[self.mongoColletion]

            url = 'http://localhost:9032/'
            jsonVars = {
                "logon":{   
                    "user":"anderakooken",
                    "passwd":"3eaa2ac727c5bca600e52483dc86d05e"
                },
                "function":"folha_"+self.empresa,
                "param":{ 
                    "folha" : self.folha
                }
            }
            headers = {'Authorization' : "", 'Accept' : 'application/json', 'Content-Type' : 'application/json'}
            request = requests.post(url, json=jsonVars, headers=headers)
            jsonReturn = json.loads(request.content)

            collection.insert_one(jsonReturn)

            print("Importação - executada")
        except :
            print("Falha na Importação")

database = "GrupoMS"
collection = "FolhaPessoal"
empresa = "MSR"
folha = "14554"

out = jsonToMongo(database, collection, empresa, folha).importTo()
