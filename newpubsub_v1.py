import board
import timeimport board
import time
import adafruit_dht
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import json
import mysql.connector
import time
import requests

def mqtt_connect():
        try:
                # For certificate based connection
                myMQTTClient = AWSIoTMQTTClient("myClientID")
                # For Websocket connection

                # Configurations
                # For TLS mutual authentication
                myMQTTClient.configureEndpoint("a2d7b4vsi90j5r-ats.iot.eu-north-1.amazonaws.com", 8883)
                # For Websocket

                myMQTTClient.configureCredentials("/home/raspberrypi/awsiot/aws/RootCA1.pem", "/home/raspberrypi/awsiot/aws/private.pem.key", "/home/raspberrypi/awsiot/aws/certificate.pem.crt")
                # For Websocket, we only need to configure the root CA

                myMQTTClient.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
                myMQTTClient.configureDrainingFrequency(2)  # Draining: 2 Hz
                myMQTTClient.configureConnectDisconnectTimeout(10)  # 10 sec
                myMQTTClient.configureMQTTOperationTimeout(5)  # 5 sec

                myMQTTClient.connect()
                return myMQTTClient
                
        except RuntimeError as error:
                        print(error.args[0])
                        time.sleep(5)
                        myMQTTClient.disconnect()
                
def connect_to_mysql_online(db,temp,hum,ts,ver):
        try:
                cursor=db.cursor()
                query="insert into mytable(Temperature,Humidity,Timestamp,Version) values (%s,%s,%s,%s)"
                cursor.execute(query,(temp,hum,ts,ver))
                cursor.close()
                db.commit()
                print("data saved to AWS database")
        except mysql.connect.Error as error:
                print("failed to connect to aws_online_database:{}".format(error))
                exit()

def connect_to_mysql_offline(temp,hum,ts,ver):
        try:
                db =mysql.connector.connect(host="localhost",user="myuser2",password="mysql",database="mydb2")
                cursor=db.cursor()
                query="insert into mytable2(Temperature,Humidity,Timestamp,Version) values (%s,%s,%s,%s)"
                cursor.execute(query,(temp,hum,ts,ver))
                cursor.close()
                db.commit()
                print("data saved to local database")
        except mysql.connect.Error as error:
                print("failed to connect to mysql_offline_database:{}".format(error))
                exit()

def mysql_data_count():
        try:
                db =mysql.connector.connect(host="localhost",user="myuser2",password="mysql",database="mydb2")
                cursor=db.cursor()
                query="SELECT COUNT(*) FROM mytable2"
                cursor.execute(query)
                result = cursor.fetchone()
                cursor.close()
                db.commit()
                return result
        except mysql.connect.Error as error:
                print("failed to connect to mysql_offline_database:{}".format(error))
                exit()

def mysql_fetch_data():
        try:
                mydb =mysql.connector.connect(host="localhost",user="myuser2",password="mysql",database="mydb2")
                mycursor = mydb.cursor()
                mycursor.execute("SELECT * FROM mytable2")
                rows = mycursor.fetchall()
                mydb.close()
                mycursor.close()
                return rows
        except mysql.connect.Error as error:
                print("failed to connect to mysql_offline_database:{}".format(error))
                exit()
      
def mysql_delete_data():
        try:
                mydb = mysql.connector.connect(host="localhost", user="myuser2", password="mysql", database="mydb2")
                mycursor = mydb.cursor()
                mycursor.execute("DELETE FROM mytable2")
                mydb.commit()
                mycursor.close()
                mydb.close()
                print("Data deleted from local database")
        except mysql.connector.Error as error:
                print("Failed to connect to mysql_offline_database: {}".format(error))
                exit()

def check_internet():
    try:
        response = requests.get("https://www.google.com", timeout=5)
        response.raise_for_status()
        return True
    except:
        return False

def main(flag):
        dhtDevice = adafruit_dht.DHT11(board.D17)
        data_aws={"Sr_no":0,"Temperature": 0,"Humidity": 0,"Version": 0,"Timestamp": 0}
        while(True):
                if(flag==1 and check_internet()== True):
                        db =mysql.connector.connect(host="database-1.cap0bv0hngpg.ap-south-1.rds.amazonaws.com",user="database_1",password="mydatabase",database="database_1")
                        myMQTTClient=mqtt_connect()
                        
                        count=mysql_data_count()
                        if(count[0]!=0):
                                print("data tranfering from local to aws database...")
                                rows=mysql_fetch_data()
                                data = []
                                for row in rows:
                                        data={"Temperature": row[1],"Humidity": row[2],"Version": row[3],"Timestamp": row[4]}
                                        connect_to_mysql_online(db,data["Temperature"],data["Humidity"],data["Timestamp"],data["Version"])
                                mysql_delete_data()
                        
                if(check_internet()== True):
                        
                        try:
                                data_aws["Sr_no"]=data_aws["Sr_no"]+1
                                data_aws["Temperature"]=str(dhtDevice.temperature)+" C /"+str(round(dhtDevice.temperature*(9/5)+32))+" F"
                                data_aws["Humidity"]=str(dhtDevice.humidity)+" %"
                                data_aws["Timestamp"]=str(round(time.time()))
                                data_aws["Version"]="Rushabh"
                        except RuntimeError as error:
                                print(error.args[0])
                                print("dht error")
               
                        data_temp = json.dumps(data_aws)
        
                        myMQTTClient.publish(topic="myTopic",QoS=1,payload=data_temp)
                        connect_to_mysql_online(db,data_aws["Temperature"],data_aws["Humidity"],data_aws["Timestamp"],data_aws["Version"])
                        time.sleep(10)
                        flag=0
                else:
                        print("internet not available")
                        flag=1
                        try:
                                data_local={"Temperature" : 0,"Humidity":0,"Timestamp":0,"Version":0}
                                data_local["Temperature"]=str(dhtDevice.temperature)+" C /"+str(round(dhtDevice.temperature*(9/5)+32))+" F"
                                data_local["Humidity"]=str(dhtDevice.humidity)+" %"
                                data_local["Timestamp"]=str(round(time.time()))
                                data_local["Version"]="Rushabh"
                                connect_to_mysql_offline(data_local["Temperature"],data_local["Humidity"],data_local["Timestamp"],data_local["Version"])
                                time.sleep(10)
                        except RuntimeError as error:
                                print(error.args[0])
                                print("dht error")
               
if __name__=="__main__":
        flag=1
        main(flag)

import adafruit_dht
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import json
import mysql.connector
import time
import requests

def mqtt_connect():
        try:
                # For certificate based connection
                myMQTTClient = AWSIoTMQTTClient("myClientID")
                # For Websocket connection

                # Configurations
                # For TLS mutual authentication
                myMQTTClient.configureEndpoint("a2d7b4vsi90j5r-ats.iot.eu-north-1.amazonaws.com", 8883)
                # For Websocket

                myMQTTClient.configureCredentials("/home/raspberrypi/awsiot/aws/RootCA1.pem", "/home/raspberrypi/awsiot/aws/private.pem.key", "/home/raspberrypi/awsiot/aws/certificate.pem.crt")
                # For Websocket, we only need to configure the root CA

                myMQTTClient.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
                myMQTTClient.configureDrainingFrequency(2)  # Draining: 2 Hz
                myMQTTClient.configureConnectDisconnectTimeout(10)  # 10 sec
                myMQTTClient.configureMQTTOperationTimeout(5)  # 5 sec

                myMQTTClient.connect()
                return myMQTTClient
                
        except RuntimeError as error:
                        print(error.args[0])
                        time.sleep(5)
                        myMQTTClient.disconnect()
                
def connect_to_mysql(db,temp,hum,ts,ver):
        try:
                cursor=db.cursor()
                query="insert into mytable(Temperature,Humidity,Timestamp,Version) values (%s,%s,%s,%s)"
                cursor.execute(query,(temp,hum,ts,ver))
                cursor.close()
                db.commit()
                print("data saved to database")
        except mysql.connect.Error as error:
                print("failed to connect to database:{}".format(error))
                exit()

def check_internet():
    try:
        response = requests.get("https://www.google.com", timeout=5)
        response.raise_for_status()
        return True
    except:
        return False

def main(flag):
        dhtDevice = adafruit_dht.DHT11(board.D17)
        data = {"Sr_no":0, "Temperature" : 0,"Humidity":0,"Timestamp":0,"Version":0}
        while(True):
                if(flag==1 and check_internet()== True):
                        db =mysql.connector.connect(host="database-1.cap0bv0hngpg.ap-south-1.rds.amazonaws.com",user="database_1",password="mydatabase",database="database_1")
                        myMQTTClient=mqtt_connect()
        
                if(check_internet()== True):
                        try:
                                data["Sr_no"]=data["Sr_no"]+1
                                data["Temperature"]=str(dhtDevice.temperature)+" C /"+str(round(dhtDevice.temperature*(9/5)+32))+" F"
                                data["Humidity"]=str(dhtDevice.humidity)+" %"
                                data["Timestamp"]=str(round(time.time()))
                                data["Version"]="Rushabh"
                        except RuntimeError as error:
                                print(error.args[0])
                                print("dht error")
               
                        data_temp = json.dumps(data)
        
                        myMQTTClient.publish(topic="myTopic",QoS=1,payload=data_temp)
                        connect_to_mysql(db,data["Temperature"],data["Humidity"],data["Timestamp"],data["Version"])
                        time.sleep(10)
                        flag=0
                else:
                        print("internet not available")
                        flag=1
if __name__=="__main__":
        flag=1
        main(flag)
