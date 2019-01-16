#!/usr/bin/python
# coding= utf-8

import json, paho.mqtt.client as mqtt
import time

cpt_events=0

cpt_packets=500
cpt_alerts=500

cpt_IPlayer=500
cpt_L4layer=500
cpt_UDPlayer=500
cpt_TCPlayer=500
cpt_ICMPlayer=500
cpt_HTTPapp=500

cpt_stream=500
cpt_L3stream=500
cpt_ICMPstream=500
cpt_L4stream=500
cpt_TCPstream=500
cpt_UDPstream=500


def create_rdf_event(jsonObj):
	#Types
	evt ="http://tridso.org/events/"

	pkt ="http://tridso.org/events/packets/"
	alrt ="http://tridso.org/events/alerts/"

	ip ="http://tridso.org/events/IPLayers/"

	l4 ="http://tridso.org/events/IPLayers/4thLayers/"
	icmp ="http://tridso.org/events/IPLayers/ICMPLayers/"

	tcp ="http://tridso.org/events/IPLayers/4thLayers/TCPLayers/"
	udp ="http://tridso.org/events/IPLayers/4thLayers/UDPLayer/"

	http ="http://tridso.org/events/IPLayers/4thLayers/HTTPApps/"

	str_rdf ="http://tridso.org/string/"
	int_rdf ="http://tridso.org/int/"


	#Properties
	hasPacket = "http://tridso.org/events/hasPacket"
	hasAlert = "http://tridso.org/events/hasAlert"
	hasTimestamp = "http://tridso.org/events/hasTimestamp"

 	hasCategory = "http://tridso.org/events/alerts/hasCategory"
 	hasSignature = "http://tridso.org/events/alerts/hasSignature"
 	actionPerformed = "http://tridso.org/events/alerts/actionPerformed"
 	hasSeverity = "http://tridso.org/events/alerts/hasSeverity"

	hasIpLayer = "http://tridso.org/events/hasIPLayer"

	hasL4Layer = "http://tridso.org/events/IPLayers/has4thLayer"
	hasICMPLayer = "http://tridso.org/events/IPLayers/hasICMPLayer"
	hasSrcIP = "http://tridso.org/events/IPLayers/hasSrcIP"
	hasDstIP = "http://tridso.org/events/IPLayers/hasDstIP"
	hasUDPLayer = "http://tridso.org/events/IPLayers/4thLayers/hasUDPLayer"
	hasTCPLayer = "http://tridso.org/events/IPLayers/4thLayers/hasTCPLayer"
	hasDstPort = "http://tridso.org/events/IPLayers/4thLayers/hasDstPort"
	hasSrcPort = "http://tridso.org/events/IPLayers/4thLayers/hasSrcPort"
	hasICMPType = "http://tridso.org/events/IPLayers/ICMPLayers/hasICMPType"

	hasHTTPapp = "http://tridso.org/events/IPLayers/TCPLayers/HTTPApps/hasHTTPApp"
	hasHostName = "http://tridso.org/events/IPLayers/TCPLayers/HTTPApps/hasHostName"
	hasURL = "http://tridso.org/events/IPLayers/TCPLayers/HTTPApps/hasURL"
	hasVersion = "http://tridso.org/events/IPLayers/TCPLayers/HTTPApps/hasVersion"
	hasMethod = "http://tridso.org/events/IPLayers/TCPLayers/HTTPApps/hasMethod"
	hasStatus = "http://tridso.org/events/IPLayers/TCPLayers/HTTPApps/hasStatus"
	hasLength = "http://tridso.org/events/IPLayers/TCPLayers/HTTPApps/hasLength"
	hasUserAgent = "http://tridso.org/events/IPLayers/TCPLayers/HTTPApps/hasUserAgent"
	hasContentType = "http://tridso.org/events/IPLayers/TCPLayers/HTTPApps/hasContentType"

	#counters
	global cpt_events

	global cpt_packets
	global cpt_alerts

	global cpt_IPlayer
	global cpt_L4layer
	global cpt_UDPlayer
	global cpt_TCPlayer
	global cpt_ICMPlayer
	global cpt_HTTPapp

	global cpt_stream
	global cpt_L3stream
	global cpt_ICMPstream
	global cpt_L4stream
	global cpt_TCPstream
	global cpt_UDPstream

	###############1. Gather necessary values from JSON object###################################################

	#Get event type (packet or alert)
	eve_type = jsonObj['event_type']
	check_l4_layer = False
	check_ICMP = False
	check_flow = True 	#remember flows


	#Get timestamp
	timestamp = jsonObj['timestamp']

	#Get protocol
	proto = jsonObj['proto']
	if proto == "UDP" or proto == "TCP":
		check_l4_layer = True
	elif proto == "ICMP":
		check_icmp=True

	#Get source ip and port if it exists
	src_ip = jsonObj['src_ip']
		
	#Get destination ip and port if it exists
	dst_ip = jsonObj['dest_ip']


	####################2. Convert event/packet common properties to rdf triples####################################
	
	#Creating event uri
	cpt_events+=1
	event = evt + "event"+str(cpt_events)

	####Event props####
	#timestamp prop triple
	event_timestamp_prop = event + " " + hasTimestamp + " " + str_rdf+timestamp + '\n'
	if eve_type == 'alert' : 
		cpt_alerts+=1
		alert_uri = alrt+"alert"+str(cpt_alerts)
	#Alert prop triple
	event_alert_prop = event + " " + hasAlert + " " + alert_uri + '\n'

	####Alert props####
	#Creating ip_layer uri
	cpt_IPlayer+=1
	ip_layer_uri = ip + "IPlayer"+str(cpt_IPlayer)
	#If event is an alert, we gather all alert specific fields and convert them to rdf triples
	category_prop = alert_uri + " " + hasCategory + " " + str_rdf+(jsonObj['alert']['category']).replace(" ","_") + '\n'
	signature_prop = alert_uri + " " + hasSignature + " " + str_rdf+(jsonObj['alert']['signature']).replace(" ","_") + '\n'
	severity_prop = alert_uri + " " + hasSeverity + " " + int_rdf+str(jsonObj['alert']['severity']) + '\n'
	action_prop = alert_uri + " " + actionPerformed + " " + str_rdf+(jsonObj['alert']['action']).replace(" ","_") + '\n'
	#IP layer triple
	alert_ip_prop = alert_uri + " " + hasIpLayer + " " + ip_layer_uri + '\n'
	
	#####IP layer props####
	#src/dst IP triples
	src_ip_prop = ip_layer_uri + " " + hasSrcIP + " " + str_rdf+src_ip + '\n'
	dst_ip_prop = ip_layer_uri + " " + hasDstIP + " " + str_rdf+dst_ip + '\n'
	#Check 4th layer presence

	ip_l4_prop=l4_protocol_prop=proto_dst_port_prop=proto_src_port_prop=""
	ip_icmp_prop=icmp_type_prop=""
	http_uri=http_all_props=""
	tcp_http_prop=""
	if check_l4_layer :
		#Creating Layer 4 uri
		cpt_L4layer+=1
		layer4_uri = l4 + "4thLayer"+str(cpt_L4layer)
		#4th layer uri
		ip_l4_prop = ip_layer_uri + " " + hasL4Layer + " " + layer4_uri + '\n'

		####4th Layer props####
		#TCP or UDP prop uri
		src_port = jsonObj['src_port']
		dst_port = jsonObj['dest_port']
		proto_uri=""
		if proto == "UDP" :
			cpt_UDPlayer+=1
			proto_uri = udp + "udpLayer"+str(cpt_UDPlayer)
			l4_protocol_prop = layer4_uri + " " + hasUDPLayer + " " + proto_uri + '\n'
		elif proto == "TCP":
			cpt_TCPlayer+=1
			proto_uri = tcp + "tcpLayer"+str(cpt_TCPlayer)
			l4_protocol_prop = layer4_uri + " " + hasTCPLayer + " " + proto_uri + '\n'

			#HTTP prop uri
			if "http" in jsonObj :
				cpt_HTTPapp+=1
				http_uri = http + "HTTPApp"+str(cpt_HTTPapp)
				tcp_http_prop = proto_uri + " " + hasHTTPapp + " " + http_uri + '\n'


				####HTTP application props####
				method = jsonObj['http']['http_method']
				url = jsonObj['http']['url']
				length = jsonObj['http']['length']
				version = jsonObj['http']['protocol'].replace(' ','_')

				http_method_prop = http_uri + " " + hasMethod + " " + str_rdf+method + '\n'
				http_url_prop = http_uri + " " + hasURL + " " + str_rdf+url + '\n'
				http_length_prop = http_uri + " " + hasLength + " " + int_rdf+str(length) + '\n'
				http_version_prop = http_uri + " " + hasVersion + " " + str_rdf+version + '\n'
				
				#Optional props
				http_status_prop=""
				if 'status' in jsonObj['http']:
					status = jsonObj['http']['status']
					http_status_prop = http_uri + " " + hasStatus + " " + int_rdf+str(status) + '\n'
				http_user_agent_prop=""
				if 'http_user_agent' in jsonObj['http']:
					user_agent = jsonObj['http']['http_user_agent'].replace(' ','_')
					http_user_agent_prop = http_uri + " " + hasUserAgent + " " + str_rdf+user_agent + '\n'
				http_content_type_prop=""
				if 'http_content_type' in jsonObj['http']:
					content_type = jsonObj['http']['http_content_type'].replace(' ','_')
					http_content_type_prop = http_uri + " " + hasContentType + " " + str_rdf+content_type + '\n'
				http_hostname_prop=""
				if 'hostname' in jsonObj['http']:
					hostname = jsonObj['http']['hostname']
					http_hostname_prop = http_uri + " " + hasHostName + " " + str_rdf+hostname + '\n'

				
				http_all_props = http_method_prop + http_hostname_prop + http_url_prop + http_length_prop + http_status_prop + http_version_prop + http_user_agent_prop + 							 http_content_type_prop
				
		####TCP/UDP Protocol props####
		proto_src_port_prop = proto_uri + " " + hasSrcPort + " " + int_rdf+str(src_port) + '\n'
		proto_dst_port_prop = proto_uri + " " + hasDstPort + " " + int_rdf+str(dst_port) + '\n'

	elif check_icmp :
		cpt_ICMPlayer+=1
		icmp_uri = icmp + "ICMP"+str(cpt_ICMPlayer)
		ip_icmp_prop = ip_layer_uri + " " + hasICMPLayer + " " + icmp_uri + '\n'
		icmp_types = {'0': 'echo_reply', '3': 'destination_unreachable', '8': 'echo_request', '5': 'redirect_message', 									     	                           '9': 'router_advertisment', '10': 'router_soliciation', '12': 'bad_ip_header', '14': 'timestamp_request', '15': 'timestamp_reply' }

		icmp_type_prop = icmp_uri + " " + hasICMPType+ " " + str_rdf+icmp_types[str(jsonObj['icmp_type'])]+ '\n'
		

	# merge all rdf triples in a single string
	alert_rdf = event_timestamp_prop + event_alert_prop + category_prop + signature_prop + severity_prop + action_prop + alert_ip_prop + src_ip_prop + 						   	                dst_ip_prop + ip_l4_prop + l4_protocol_prop + ip_icmp_prop + icmp_type_prop + proto_src_port_prop + proto_dst_port_prop + tcp_http_prop + http_all_props


	print alert_rdf
	return alert_rdf.replace(":","=")

		


# The callback for when the sensor receives a CONNACK response from the server.
def on_connect(sensor, userdata, flags, rc):
    print("Connected with result code "+str(rc))

# The callback for when a PUBLISH message is received from the server.
def on_receive(sensor, userdata, msg):
    print("Message received : "+msg.topic+" "+str(msg.payload))

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))



sensor = mqtt.Client()
sensor.on_connect = on_connect
sensor.on_message = on_message

sensor.connect("localhost", 1883, 60)
topic = "alerts"


json_events = open("/var/log/suricata/eve.json")
event_str = json_events.readline()
events_rdf=""

while True :
	if event_str :
		#print event_str

		if event_str != '\n' :	
			event = json.loads(event_str)

			if event['event_type'] == "alert" :
				events_rdf+=create_rdf_event(event)
				#break

	else : 
		if events_rdf :
			print events_rdf
			print "Publishing alerts..." 
			sensor.publish(topic,events_rdf[:-1])
			events_rdf = ""
		time.sleep(5)

	event_str = json_events.readline()







