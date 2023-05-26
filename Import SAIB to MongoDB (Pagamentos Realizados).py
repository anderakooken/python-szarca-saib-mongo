import pymongo
import json
import requests

class jsonToMongo:

    def __init__(self, database, mongoColletion, empresa, data, range):
        self.database = database
        self.mongoColletion = mongoColletion
        self.empresa = empresa
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
                "function":"despesasMovimento",
                "param":{ 
                    "empresa" : self.empresa,
                    "dataInicial" : dataSelecionada,
                    "dataFinal" : dataSelecionada
                }
            }
            headers = {'Authorization' : "", 'Accept' : 'application/json', 'Content-Type' : 'application/json'}
            request = requests.post(url, json=jsonVars, headers=headers)
            jsonReturn = json.loads(request.content)

            collection.insert_one(jsonReturn)

            print("OK: Empresa: "+self.empresa+", Dia: "+dataSelecionada)
        except :
            print("Falha na Importação: Empresa: "+self.empresa+", Dia: "+dataSelecionada)
    
    def req(self):

        for i in self.range:

            dia  = "{:02}".format(i)

            try:
                self.importTo(dia+"/"+self.data)
            except:
                print ("Não havia dados para o data: "+dia+"/"+self.data)

database = "GrupoMS"
collection = "Pagamentos"

#for ano in ["2015","2016","2017","2018","2019","2020","2021","2022"]:

for ano in ["2023"]:
    for empresa in ["10","31","41"]:

        jsonToMongo(database, collection, empresa, "01/"+ano, range(1,32)).req()
        jsonToMongo(database, collection, empresa, "02/"+ano, range(1,29)).req()
        jsonToMongo(database, collection, empresa, "03/"+ano, range(1,32)).req()
        jsonToMongo(database, collection, empresa, "04/"+ano, range(1,31)).req()
        '''jsonToMongo(database, collection, empresa, "05/"+ano, range(1,32)).req()
        jsonToMongo(database, collection, empresa, "06/"+ano, range(1,31)).req()
        jsonToMongo(database, collection, empresa, "07/"+ano, range(1,32)).req()
        jsonToMongo(database, collection, empresa, "08/"+ano, range(1,32)).req()
        jsonToMongo(database, collection, empresa, "09/"+ano, range(1,31)).req()
        jsonToMongo(database, collection, empresa, "10/"+ano, range(1,32)).req()
        jsonToMongo(database, collection, empresa, "11/"+ano, range(1,31)).req()
        jsonToMongo(database, collection, empresa, "12/"+ano, range(1,32)).req()
        '''


   

