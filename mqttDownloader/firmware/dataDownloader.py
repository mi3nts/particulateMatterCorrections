






import threading
import paho.mqtt.client as mqtt
import ast
import datetime
import yaml
import collections
import json
import ssl
from mintsXU4 import mintsSensorReader as mSR
from mintsXU4 import mintsDefinitions as mD
from mintsXU4 import mintsLatest as mL
from mintsXU4 import mintsLoRaReader as mLR

import sys
import pandas as pd


########### This part was written by Prabu ############
# install {pip install scikit-learn==1.2.2}
from sklearn.ensemble import ExtraTreesRegressor
from sklearn.model_selection import train_test_split
import joblib
from datetime import timedelta

from humidCorr_step1 import humid 

climateSensor = 'BME280V2'
climateDataDic = {}
######################################################

# Common Inputs
mqttCredentialsFile   = mD.credentials
tlsCert               = mD.tlsCertsFile

sensorInfo            = mD.sensorInfo
credentials           = mD.credentials

portInfo              = mD.portInfo
nodeInfo              = mD.nodeInfo

connected             = False  # Stores the connection status

nodeIDs               = nodeInfo['nodeIDs']
sensorIDs             = sensorInfo['sensorID']

## DC Inputs
mqttPortDC              = mD.mqttPortDC
mqttBrokerDC            = mD.mqttBrokerDC

mqttUNDC                = credentials['mqtt']['username'] 
mqttPWDC                = credentials['mqtt']['password'] 

## LN Inputs
mqttPortLN              = mD.mqttPortLoRa
mqttBrokerLN            = mD.mqttBrokerLoRa

mqttUNLN                = credentials['LoRaMqtt']['username'] 
mqttPWLN                = credentials['LoRaMqtt']['password'] 



decoder = json.JSONDecoder(object_pairs_hook=collections.OrderedDict)


# The callback for when the client receives a CONNACK response from the server.
def on_connect_DC(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
 
    # Subscribing in on_connect() - if we lose the connection and
    # reconnect then subscriptions will be renewed.
    for nodeID in nodeIDs:
        for sensorID in sensorIDs:
            topic = nodeID+"/"+ sensorID
            client.subscribe(topic)
            print("Subscrbing to Topic: "+ topic)

# The callback for when a PUBLISH message is received from the server.
def on_message_DC(client, userdata, msg):
    print()
    print(" - - - MINTS DATA RECEIVED - - - ")
    print()
    # print(msg.topic+":"+str(msg.payload))
    try:
        [nodeID,sensorID] = msg.topic.split('/')
        sensorDictionary = decoder.decode(msg.payload.decode("utf-8","ignore"))
        print("Node ID   :" + nodeID)
        print("Sensor ID :" + sensorID)
        print("Data      : " + str(sensorDictionary))


        ########### This part was written by Prabu ############

        loaded_humidModel = joblib.load("/home/teamlary/Prabu/analysis/HumidCorr_onSensor/humidCorr_model/_impl_sensor.joblib")
        
        def make_prediction2(modelName, est_df):
            predictions_train = pd.DataFrame(modelName.predict(est_df),columns=["Predictions"])
            return predictions_train   
        
        def climateSensor_case(argument):
            if argument == 'BME280V2':
                climateDataDic['dateTime'] = sensorDictionary['dateTime']
                climateDataDic['temperature'] = sensorDictionary['temperature']
                climateDataDic['pressure'] = sensorDictionary['pressure']
                climateDataDic['humidity'] = sensorDictionary['humidity']
                climateDataDic['dewPoint'] = sensorDictionary['dewPoint']
            elif argument == 'WIMDA':
                climateDataDic['dateTime'] = sensorDictionary['dateTime']
                climateDataDic['temperature'] = sensorDictionary['airTemperature']
                climateDataDic['pressure'] = 1000*float(sensorDictionary['barrometricPressureBars'])
                climateDataDic['humidity'] = sensorDictionary['relativeHumidity']
                climateDataDic['dewPoint'] = sensorDictionary['dewPoint']
            else:
                print('Not a climate sensor')
        
        climateSensor_case(sensorID)

        print('***************************************************************************************')
        print(climateDataDic)

        if sensorID == "IPS7100" and bool(climateDataDic):
            IPS7100_dateTime = sensorDictionary['dateTime']
            pc0_1, pc0_3, pc0_5, pc1_0, pc2_5, pc5_0, pc10_0 = sensorDictionary['pc0_1'], sensorDictionary['pc0_3'], sensorDictionary['pc0_5'], sensorDictionary['pc1_0'], sensorDictionary['pc2_5'], sensorDictionary['pc5_0'], sensorDictionary['pc10_0']
            climate_datetime, humidity, temperature, dewPoint, pressure = climateDataDic['dateTime'], climateDataDic['humidity'], climateDataDic['temperature'], climateDataDic['dewPoint'], climateDataDic['pressure']
            foggy = float(temperature) - float(dewPoint)
            print(humidity, temperature, dewPoint, pressure, foggy)

            timestamp_IPS = datetime.datetime.strptime(str(IPS7100_dateTime), "%Y-%m-%d %H:%M:%S.%f")
            timestamp_clim = datetime.datetime.strptime(str(climate_datetime), "%Y-%m-%d %H:%M:%S.%f")
            time_difference = abs(timestamp_IPS - timestamp_clim)
            ten_minutes = timedelta(minutes=10)
            print('..............................')
            print(time_difference)
            print(ten_minutes)
            if time_difference < ten_minutes:
                cor_pc0_1, cor_pc0_3, cor_pc0_5, cor_pc1_0, cor_pc2_5, cor_pc5_0, cor_pc10_0, humidity, temperature, dewPoint  = humid(pc0_1, pc0_3, pc0_5, pc1_0, pc2_5, pc5_0, pc10_0, humidity, temperature, dewPoint)
            
                m0_1 = 8.355696123812269e-07
                m0_3 = 2.2560825222215327e-05
                m0_5 = 0.00010446111749483851
                m1_0 = 0.0008397941861044865
                m2_5 = 0.013925696906339288
                m5_0 = 0.12597702778750686
                m10_0 = 1.0472
                
                cor_pm0_1 = m0_1*cor_pc0_1
                cor_pm0_3 = cor_pm0_1 + m0_3*cor_pc0_3
                cor_pm0_5 = cor_pm0_3 + m0_5*cor_pc0_5
                cor_pm1_0 = cor_pm0_5 + m1_0*cor_pc1_0
                cor_pm2_5 = cor_pm1_0 + m2_5*cor_pc2_5
                cor_pm5_0 = cor_pm2_5 + m5_0*cor_pc5_0
                cor_pm10_0 = cor_pm5_0 + m10_0*cor_pc10_0
                print(cor_pm0_1, cor_pm0_3, cor_pm0_5, cor_pm1_0, cor_pm2_5, cor_pm5_0, cor_pm10_0)

                ##### ML humidity correction #############################
                #predictors = ['cor_pm2_5', 'temperature', 'pressure', 'humidity', 'dewPoint', 'altitude']
                data = {'cor_pm2_5': [float(cor_pm2_5)], 'temperature': [float(temperature)], 'pressure': [pressure], 'humidity':[humidity], 'dewPoint':[dewPoint], 'temp_dew':[foggy]}
                #data = {'cor_pm2_5': [float(cor_pm2_5)], 'temperature': [float(temperature)]}
                df = pd.DataFrame(data)
                print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
                #print(df)
                predicted_train_valid2 = make_prediction2(loaded_humidModel, df)
                ML_pm2_5 = predicted_train_valid2["Predictions"][0]


                corr_data = {'ori_pm2_5': sensorDictionary['pm2_5'], 'HG_pm2_5': cor_pm2_5, 'HG_ML_pm2_5': ML_pm2_5}
                print(corr_data)
            else:
                print("The time difference is greater than or equal to 10 minutes.")
            #print(timestamp_clim)
        
            '''
            cor_pc0_1, cor_pc0_3, cor_pc0_5, cor_pc1_0, cor_pc2_5, cor_pc5_0, cor_pc10_0, humidity, temperature, dewPoint  = humid(pc0_1, pc0_3, pc0_5, pc1_0, pc2_5, pc5_0, pc10_0, humidity, temperature, dewPoint)
            
            m0_1 = 8.355696123812269e-07
            m0_3 = 2.2560825222215327e-05
            m0_5 = 0.00010446111749483851
            m1_0 = 0.0008397941861044865
            m2_5 = 0.013925696906339288
            m5_0 = 0.12597702778750686
            m10_0 = 1.0472

            cor_pm0_1 = m0_1*cor_pc0_1
            cor_pm0_3 = cor_pm0_1 + m0_3*cor_pc0_3
            cor_pm0_5 = cor_pm0_3 + m0_5*cor_pc0_5
            cor_pm1_0 = cor_pm0_5 + m1_0*cor_pc1_0
            cor_pm2_5 = cor_pm1_0 + m2_5*cor_pc2_5
            cor_pm5_0 = cor_pm2_5 + m5_0*cor_pc5_0
            cor_pm10_0 = cor_pm5_0 + m10_0*cor_pc10_0
            print(cor_pm0_1, cor_pm0_3, cor_pm0_5, cor_pm1_0, cor_pm2_5, cor_pm5_0, cor_pm10_0)

            ##### ML humidity correction #############################
            #predictors = ['cor_pm2_5', 'temperature', 'pressure', 'humidity', 'dewPoint', 'altitude']
            data = {'cor_pm2_5': [float(cor_pm2_5)], 'temperature': [float(temperature)], 'pressure': [pressure], 'humidity':[humidity], 'dewPoint':[dewPoint], 'temp_dew':[foggy]}
            #data = {'cor_pm2_5': [float(cor_pm2_5)], 'temperature': [float(temperature)]}
            df = pd.DataFrame(data)
            print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
            #print(df)
            predicted_train_valid2 = make_prediction2(loaded_humidModel, df)
            ML_pm2_5 = predicted_train_valid2["Predictions"][0]


            corr_data = {'ori_pm2_5': sensorDictionary['pm2_5'], 'HG_pm2_5': cor_pm2_5, 'HG_ML_pm2_5': ML_pm2_5}
            print(corr_data)
            '''

        else:
            print('Note: Not IPS7100 or climateDataDic is empty')
            
         
        
        
        
        ######################################################

        if sensorID== "FRG001":
            dateTime  = datetime.datetime.strptime(sensorDictionary["dateTime"], '%Y-%m-%d %H:%M:%S')
        else:
            dateTime  = datetime.datetime.strptime(sensorDictionary["dateTime"], '%Y-%m-%d %H:%M:%S.%f')
        writePath = mSR.getWritePathMQTT(nodeID,sensorID,dateTime)
        exists    = mSR.directoryCheck(writePath)
        sensorDictionary = decoder.decode(msg.payload.decode("utf-8","ignore"))
        print("Writing MQTT Data")
        print(writePath)
        mSR.writeCSV2(writePath,sensorDictionary,exists)
        mL.writeJSONLatestMQTT(sensorDictionary,nodeID,sensorID)

    except Exception as e:
        print("[ERROR] Could not publish data, error: {}".format(e))



def on_connect_LN(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    topic = "utd/lora/app/5/device/+/event/up"
    client.subscribe(topic)
    print("Subscrbing to Topic: "+ topic)

def on_message_LN(client, userdata, msg):
    try:
        print("==================================================================")
        print(" - - - MINTS DATA RECEIVED - - - ")
        # print(msg.payload)
        dateTime,gatewayID,nodeID,sensorID,framePort,base16Data = \
            mLR.loRaSummaryWrite(msg,portInfo)
        

        print("Node ID         : " + nodeID)
        print("Sensor ID       : " + sensorID)
        if nodeID in nodeIDs:
            print("Date Time       : " + str(dateTime))
            print("Port ID         : " + str(framePort))
            print("Base 16 Data    : " + base16Data)
            mLR.sensorSendLoRa(dateTime,nodeID,sensorID,framePort,base16Data)
        
    
    except Exception as e:
        print("[ERROR] Could not publish data, error: {}".format(e))


# Create an MQTT client and attach our routines to it.
def mqtt_client_DC():       
    clientDC = mqtt.Client()
    clientDC.on_connect = on_connect_DC
    clientDC.on_message = on_message_DC
    clientDC.username_pw_set(mqttUNDC,mqttPWDC)

    clientDC.tls_set(ca_certs=tlsCert, certfile=None,
                                keyfile=None, cert_reqs=ssl.CERT_REQUIRED,
                                tls_version=ssl.PROTOCOL_TLSv1_2, ciphers=None)


    clientDC.tls_insecure_set(True)
    clientDC.connect(mqttBrokerDC, mqttPortDC, 60)
    clientDC.loop_forever()


def mqtt_client_LN():       
# Create an MQTT client and attach our routines to it.
    clientLN = mqtt.Client()
    clientLN.on_connect = on_connect_LN
    clientLN.on_message = on_message_LN
    clientLN.username_pw_set(mqttUNLN,mqttPWLN)
    clientLN.connect(mqttBrokerLN, mqttPortLN, 60)
    clientLN.loop_forever()


threadDC = threading.Thread(target=mqtt_client_DC)
threadLN = threading.Thread(target=mqtt_client_LN)

# Start the threads
threadDC.start()
threadLN.start()

# Wait for all threads to finish
threadDC.join()
threadLN.join()