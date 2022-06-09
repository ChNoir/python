
import time
import paho.mqtt.client as mqtt

class struc_queue () :
    _lites = []
    
    def add (self ,element) :
        self._lites.append(element)


    def pop (self) :
        if len(self._lites) > 0 :
            return self._lites.pop(0)
        else :
            return None

class struc_msg () :
    payload = ""
    topic = ""
    QoS = 0

    def __init__(self, payload , topic , QoS = 0):
        self.payload = payload
        self.topic = topic
        self.QoS = QoS


class MQTT ( mqtt.Client ) :
    
    sub_msg = struc_queue()
    _prefix = ""
    _sub_topic = []

    def on_connect(self, mqttc, obj, flags, rc):  
        for topic in self._sub_topic :
            self.subscribe(( self._prefix + topic ))   
        print("Connected ok")

    def on_connect_fail(self, mqttc, obj):
        print("Connect failed")

    def on_message(self, mqttc, obj, msg):
        payload = str(msg.payload.decode("utf-8"))
        topic = msg.topic
        if (self._prefix != ''):
            topic = topic.replace(self._prefix,"")
        self.sub_msg.add(struc_msg(payload,topic))
        print(f"[sub] massage : {payload} , topic : {topic}")

    def prefix_topic (self , prefix ):
        self._prefix = prefix
    
    def sub_topic(self , array) :
        self._sub_topic = array

    def run(self , id_broker , port):
        self.connect(id_broker, port)
        self.loop_start ()

    def pub_msg (self, topic= '' , payload =' ', QoS = 0) :
        self.publish((self._prefix + topic) ,payload ,QoS)
        print(f"[pub] payload : {payload} , topic : {topic} , Qos {QoS}")
       
    

       

##########################################################################


pubTopic = ["code_alerte","batterie", "pong", "soleil"]
subTopic =  ["ouverture","ping","remove_poule","add_poule", "test"]

broker = '5.196.95.208' # Broker publique
port = 1883

test = MQTT()
test.prefix_topic("BTSpoulailler2022/")
test.sub_topic(subTopic)
test.run('5.196.95.208',1883)


test.pub_msg("pong","ok")

while True :
    time.sleep(1)
