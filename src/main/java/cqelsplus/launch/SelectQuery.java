package cqelsplus.launch;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.util.FileManager;

import cqelsplus.JENA.JENAOntology;
import cqelsplus.MQTT.MQTTHelper;
import cqelsplus.engine.*;
import cqelsplus.execplan.data.IMapping;
import cqelsplus.execplan.oprouters.QueryRouter;

public class SelectQuery {

	static Node rfid = Node.createURI("http://deri.org/streams/rfid"); // STREAM CLAUSE IN SPARQL QUERY
	static String CQELSHOME, CQELSDATA, QUERYNAME, STATICFILE, STREAMFILE;
    static int outAmount = 0;
    
	public static void main(String[] args) throws IOException {

		CQELSHOME = "/home/midoo/tmp/cqels";
		CQELSDATA = "/home/midoo/tmp/cqels/cqels_data";
		QUERYNAME = "query7.cqels";
		boolean STATICLOAD = true;
		STATICFILE = "attacks.rdf";
		//STREAMFILE = "rfid_10000.stream";



		Config.PRINT_LOG = true;
		FileManager filemanager = FileManager.get();
        String queryname = filemanager.readWholeFileAsUTF8(CQELSDATA+ "/" + QUERYNAME);

        final ExecContext context = new CqelsplusExecContext(CQELSHOME, true);
        
        ExecContextFactory.setExecContext(context);

        if(STATICLOAD) {
        	context.loadDefaultDataset(CQELSDATA + "/" + STATICFILE);
            //context.loadDataset("http://deri.org/floorplan/", CQELSDATA+"/floorplan.rdf");
        }


        BufferedReader reader = new BufferedReader(new FileReader(CQELSDATA + "/authors.text"));
        String name = reader.readLine();
        reader.close();

        //QueryRouter qr = context.engine().registerSelectQuery(generateQuery(queryname, name));

		QueryRouter qr = context.engine().registerSelectQuery(queryname);

        qr.addListener(new SelectListener() {
			@Override
			public void update(IMapping mapping) {
				String result = "";
				for (Var var : mapping.getVars()) {
					long value = mapping.getValue(var);
					if (value > 0) {
						result += context.engine().decode(value) + " ";
					} else {
						result += Long.toString(-value) + " ";
					}
				}
				Timestamp timestamp = new Timestamp(System.currentTimeMillis());
				outAmount++;
				String[] uris = result.split(" ");
				System.out.println("Ping scan detected ");
				System.out.println("Number of echo_requests in the last minute : " + uris[1].substring(1,2));
				System.out.println("From IP : " + uris[0].split("/")[4]);

			}
		});

        //TextStream ts1 = new TextStream(context, rfid.toString(), CQELSDATA + "/stream/" + STREAMFILE);
        //new Thread(ts1).start();


        MQTTStream mqttStream = new MQTTStream(context, rfid.toString(), CQELSDATA + "/stream/" + "tmp");

		JENAOntology jenaOntology = new JENAOntology();
		MQTTHelper mqttHelper = new MQTTHelper(mqttStream,jenaOntology);
		// MQTT listener setup
		mqttHelper.setNewListner();
		// MQTT server connection
		mqttHelper.establishConnection();
		// MQTT subscribing to a topic
		mqttHelper.subscribeToTopic("alerts");

		while (true);
	}

	private static String generateQuery(String template, String name) {
		return template.replaceAll("AUTHORNAME", name);
	}
		
	public static  Node n(String st) {
		return Node.createURI(st);
	}
}
