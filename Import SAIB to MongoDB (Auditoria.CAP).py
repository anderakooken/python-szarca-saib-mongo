import pymongo
import json
import requests

class jsonToMongo:

    def __init__(self, database, mongoColletion, data, range):
        self.database = database
        self.mongoColletion = mongoColletion
        self.data = data
        self.range = range

    def importTo(self, dataSelecionada):
    
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
                "function":"auditoria(cta_pag)",
                "param":{ 
                    "dataInicial" : dataSelecionada,
                    "dataFinal" : dataSelecionada
                }
            }
            headers = {'Authorization' : "", 'Accept' : 'application/json', 'Content-Type' : 'application/json'}
            request = requests.post(url, json=jsonVars, headers=headers)
            jsonReturn = json.loads(request.content)

            collection.insert_one(jsonReturn)

            print("OK: Dia: "+dataSelecionada)
        except :
            print("Falha na Importação: Dia: "+dataSelecionada)
    
    def req(self):

        for i in self.range:

            dia  = "{:02}".format(i)

            try:
                self.importTo(dia+"/"+self.data)
            except:
                print ("Não havia dados para o data: "+dia+"/"+self.data)


database = "GrupoMS"
collection = "Auditorias"

print ("**********[ Inicio de Função : auditoria(cta_pag) ]***************")

for ano in ["2023"] :

    jsonToMongo(database, collection, "01/"+ano, range(1,32)).req()
    jsonToMongo(database, collection, "02/"+ano, range(1,29)).req()
    jsonToMongo(database, collection, "03/"+ano, range(1,32)).req()
    jsonToMongo(database, collection, "04/"+ano, range(1,31)).req()
    jsonToMongo(database, collection, "05/"+ano, range(1,23)).req()
    #jsonToMongo(database, collection, "06/"+ano, range(1,31)).req()
    #jsonToMongo(database, collection, "07/"+ano, range(1,32)).req()
    #jsonToMongo(database, collection, "08/"+ano, range(1,32)).req()
    #jsonToMongo(database, collection, "09/"+ano, range(1,31)).req()
    #jsonToMongo(database, collection, "10/"+ano, range(1,32)).req()
    #jsonToMongo(database, collection, "11/"+ano, range(1,31)).req()
    #jsonToMongo(database, collection, "12/"+ano, range(1,32)).req()

   

