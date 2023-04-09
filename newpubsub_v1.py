import board
import time
import adafruit_dht
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import json
import mysql.connector

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

def main():
        dhtDevice = adafruit_dht.DHT11(board.D4)
        db =mysql.connector.connect(host="database-1.cap0bv0hngpg.ap-south-1.rds.amazonaws.com",user="database_1",password="mydatabase",database="database_1")
        myMQTTClient=mqtt_connect()
        data = {"Sr_no":0, "Temperature" : 0,"Humidity":0,"Timestamp":0,"Version":0}
        while(True):
                try:
                        data["Sr_no"]=data["Sr_no"]+1
                        data["Temperature"]=str(dhtDevice.temperature)+" C /"+str(round(dhtDevice.temperature*(9/5)+32))+" F"
                        data["Humidity"]=str(dhtDevice.humidity)+" %"
                        data["Timestamp"]=str(round(time.time()))
                        data["Version"]="version-1"
                except RuntimeError as error:
                        print(error.args[0])
                        print("dht error")
               
                data_temp = json.dumps(data)
                
                myMQTTClient.publish(topic="myTopic",QoS=1,payload=data_temp)
                connect_to_mysql(db,data["Temperature"],data["Humidity"],data["Timestamp"],data["Version"])
                time.sleep(10)
                
if __name__=="__main__":
        main()
