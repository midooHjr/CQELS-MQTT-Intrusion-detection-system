The goal of this project is to developpe a real-time intrusion detection and reaction system based on the sementic web technologies (RDF and RDF stream processing engine), an open source IDS and MQTT messaging protocol. 

This implementation is being developped using :

Danh Le Phuoc's original CQELS RDF stream processing engine implementation (https://code.google.com/archive/p/cqels/)
and
Chan Le Van's improvement to the engine (https://github.com/chanlevan/CQELSPLUS)

**Prerequisites** :

- Suricata IDS (https://suricata-ids.org/)

- Paho-MQTT python library (https://pypi.org/project/paho-mqtt)

- Chan Le Van's CQELSPLUS engine (https://github.com/chanlevan/CQELSPLUS)


**This implementation is composed of the following parts** : 

 - An IDS (Suricata here) that creates alerts and store them in JSON format whenever an intrusion is detected according to predefined rules (see Suricata doc for further information).
  
 - A python daemon ('jsonConverter.py') that first, converts alerts recorded by Suricata in its eve.log file to RDF format, and second, sends those converted alerts to the CQELSPLUS engine through MQTT protocol.
 
 - A CQELS Java engine (CQELSPLUS) that creates a stream with incoming RDF triples sent by the python daemon and reasonate over those according to the registred SPARQL queries.     

**Configuration** : 

**Suricata IDS** :

- Need to enable events recording in json format (log result in file "eve.json") through Suricata configuration file "suricata.yaml".

**Python deamon** :

- Need to define path to "eve.json" log file (var 'json_events' in 'jsonConverter.py').

**CQELS engine** : 

See "src/main/java/cqelsplus/launch/selectQuery.class" file. It contains an exemple of the engine's execution for a select query.

In order to properly initialize the CQELS engine the following is done :

- Creating a home directory for the CQELS engine (in selectQuery.class, its path is "tmp/cqels" in this project and it is saved in 'CQELSHOME' variable) containing two folders 'cqels/cache/' and 'cqels/datasets/'. 

- Create an URI for the new stream (in the selectQuery.class file, it's saved under the variable "rfid").

- Select the file containing the data to be streamed under the URI (variable "STREAMFILE").

- Setting a value for the boolean variable STATICLOAD, in order to indicate to the engine that a static dataset needs to be loaded, if it is the case, then the path to the file containing the data needs to be indicated.

- Setting the path to the query that needs to be registered in the CQELS engine (here it's the variable "QUERYNAME").

- The last three elements are saved under "/tmp/cqels/cqels_data" folder.

Finally, a listener is defined in order to describe the reaction to adopt when some streamed data matches the registred SPARQL query.
