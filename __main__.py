from awscrt import io, mqtt
from awsiot import mqtt_connection_builder
import json
import threading

PATH_TO_REGJSON = "/opt/cisco/registration/RegistrationDetails.json"
PATH_TO_CERTIFICATE = "/opt/cisco/registration/certificate.pem.crt"
PATH_TO_PRIVATE_KEY = "/opt/cisco/registration/private.pem.key"
PATH_TO_AMAZON_ROOT_CA_1 = "/opt/cisco/registration/AmazonRootCA1.pem"
f = open(PATH_TO_REGJSON) 
regdata = json.load(f)
f.close()
CLIENT_ID= regdata['thingName']
CUSTOMER_ID = regdata['customerId']
CONTROLPOINT_QUEUE = regdata["controlpointQueue"]
AGENTGATEWAY_QUEUE = regdata["agentGatewayQueue"]
ENDPOINT = regdata['clientEndPoint']
SUBTOPIC= regdata['subscribeTopic']
PUBTOPIC= regdata['publishTopic']
MESSAGE = json.dumps({"remoteNodeId": CLIENT_ID , "transactionType": "INITIAL-SETUP" , "topic": AGENTGATEWAY_QUEUE ,  "customerId": CUSTOMER_ID , "s3Bucket": "agentrepo/2.0.0" })
#MESSAGE = json.dumps({"remoteNodeId":CLIENT_ID, "transactionType":"CLI_UPLOADS","transactionId":"null","customerId":"82EPpi6z0QinZX","topic":"afm-cli_data","status":"Success","details":[{"message":"{\"fileName\":\"20039_SIM1296590_82EPpi6z0QinZX_1649251398253_clioutput.txt\",\"serialNumber\":\"SIM1296590\",\"prefix\":\"/st=Campus/cli/custId=82EPpi6z0QinZX/ctrlId=2563fb5c-ce22-4725-be88-7f70a790e359/date=2022-04-22/devId=9c86a46c-8bfc-327a-848e-2088b84d13c1/\",\"customerId\":\"82EPpi6z0QinZX\",\"msgId\":20039,\"ng\":\"true\"}"}]})

# Spin up resources
event_loop_group = io.EventLoopGroup(1)
host_resolver = io.DefaultHostResolver(event_loop_group)
client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)
global myMQTTClient
myMQTTClient = mqtt_connection_builder.mtls_from_path(
            endpoint=ENDPOINT,
            endpoint_port= 443 ,
            cert_filepath=PATH_TO_CERTIFICATE,
            pri_key_filepath=PATH_TO_PRIVATE_KEY,
            client_bootstrap=client_bootstrap,
            ca_filepath=PATH_TO_AMAZON_ROOT_CA_1,
            client_id=CLIENT_ID,
            clean_session=True,
            keep_alive_secs=6
            )

print("Connecting to {} with client ID '{}'...".format(
        ENDPOINT, CLIENT_ID))
# Make the connect() call
connect_future = myMQTTClient.connect()
# Future.result() waits until a result is available
connect_future.result()

def on_message_received(topic, payload, dup, qos, retain, **kwargs):
        print("Received message from topic")
        encoding = 'utf-8'
        s_msg = str(payload ,encoding )
        print(s_msg)
        # disconnect_future = myMQTTClient.disconnect()
        # disconnect_future.result()
        print("Success!")

subscribe_future, packet_id = myMQTTClient.subscribe(
            topic=SUBTOPIC,
            qos=mqtt.QoS.AT_LEAST_ONCE,
            callback=on_message_received)
subscribe_result = subscribe_future.result()
received_all_event = threading.Event()

print("Connected!")
# Publish message to server desired number of times.
print('Begin Publish')
#while True:
myMQTTClient.publish(topic=PUBTOPIC, payload=MESSAGE, qos=mqtt.QoS.AT_LEAST_ONCE)
print("Published: " + MESSAGE)
print('Publish End')
    
x= threading.Thread(target=received_all_event.wait)
x.start()