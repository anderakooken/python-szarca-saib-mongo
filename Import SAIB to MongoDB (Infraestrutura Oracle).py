import pymongo
import json
import requests
import pandas

class jsonToMongo:

    def __init__(self):

        #informa se é pra matar os processos
        self.kill = False

        #MongoDB - credenciais
        self.mongoURI = "mongodb://szarca:msabor1250@192.168.254.203:27017/?authMechanism=DEFAULT"
        self.database = "GrupoMS"
        self.mongoColletion = "Infraestrutura"

        #Szarca - credenciais
        self.url = 'https://appliance.szarca.com/szarca-api/'
        self.headers = {'Authorization' : "", 'Accept' : 'application/json', 'Content-Type' : 'application/json'}
        self.user = '@grupoms'
        self.passwd = '457c916f1a3d0fc8f5cf522b1ccd7538'

    def killSession(self, sid, serial):
        jsonVars = {
            "logon":{"user": self.user,"passwd": self.passwd},
            "function":"sessionKill(oracle)", "param":{"sid" : sid+","+serial}
        }
        
        request = requests.post(self.url, json=jsonVars, headers=self.headers)
        #print(request.content)

    def saveIntoMongo(self, json):
        server = pymongo.MongoClient(self.mongoURI)
        db = server[self.database]
        collection = db[self.mongoColletion]
        collection.insert_one(json)
        server.close()

    def emailReport(self, qtd):
        jsonVars = {
            "logon":{"user": self.user,"passwd": self.passwd},
            "function":"sessionKill(email)", "param":{"qtd" : qtd}
        }

        request = requests.post(self.url, json=jsonVars, headers=self.headers)

        print(request)

    def importTo(self):
    
        try:
            
            jsonVars = {
                "logon":{"user": self.user,"passwd": self.passwd},
                "function":"rotinasPrejudicadas", "param":{}
            }
            request = requests.post(self.url, json=jsonVars, headers=self.headers)
            jsonReturn = json.loads(request.content)

            active = False
            qtdRotinas = 0
            for i in range(len(jsonReturn["return"]["message"]["resultset"])):

                if(jsonReturn["return"]["message"]["resultset"][i]["status"] == "ACTIVE"):

                    qtdRotinas += 1

                    active = True

                    if(self.kill):

                        self.killSession(
                            jsonReturn["return"]["message"]["resultset"][i]["sid"], 
                            jsonReturn["return"]["message"]["resultset"][i]["serial"]
                        )

                        print("Processo : "+jsonReturn["return"]["message"]["resultset"][i]["sid"] + " : "+jsonReturn["return"]["message"]["resultset"][i]["serial"]+" - Encerrado.") 
   
            if(active):
                #Salva no banco de arquivos
                self.saveIntoMongo(jsonReturn)

                #comunica por e-mail
                self.emailReport(qtdRotinas)

                print("OK: Rotina executada com sucesso!")

        
            dataframe = pandas.DataFrame(jsonReturn["return"]["message"]["resultset"])
            print(dataframe)

        except Exception as e:
            print("Falha na Importação" + str(e))
    

print ("**********[                                          ]***************")
print ("          [ Inicio de Função : Infraestrutura ORACLE ]               ")
print ("**********[                                          ]***************")

jsonToMongo().importTo()


   

