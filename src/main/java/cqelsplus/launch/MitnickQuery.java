package cqelsplus.launch;


import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.util.FileManager;
import cqelsplus.JENA.JENAOntology;
import cqelsplus.MQTT.MQTTHelper;
import cqelsplus.MQTT.MQTTStream;
import cqelsplus.engine.*;
import cqelsplus.execplan.data.IMapping;
import cqelsplus.execplan.oprouters.QueryRouter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.apache.shiro.util.StringUtils.split;

public class MitnickQuery {

	static Node rfid = Node.createURI("http://deri.org/streams/rfid"); // STREAM CLAUSE IN SPARQL QUERY
	static String CQELSHOME, CQELSDATA, QUERYNAME, STATICFILE, STREAMFILE;
    static int outAmount = 0;
    
	public static void main(String[] args) throws IOException {

		CQELSHOME = "/home/midoo/tmp/cqels";
		CQELSDATA = "/home/midoo/tmp/cqels/cqels_data";
		QUERYNAME = "synFlood_query.cqels";
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

		QueryRouter qrSyn = context.engine().registerSelectQuery(queryname);

        qrSyn.addListener(new SelectListener() {
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
				System.out.println(result);

				System.out.println("Syn-Flood detected");
				String targetIP = uris[0].split("/")[4] ;
				Random randomGen = new Random();
				int sid = randomGen.nextInt(10000);

				// Write Suricata rule for spoofed IP
				try {
					String rule = "\nalert ip "+ targetIP +" any -> any any (msg: \"Spoofed IP packet\"; sid:"+sid+"; rev:1 ;)";
					// if rule does not exist yet, add it to custom.rules
					List <String> rules = new ArrayList<>(10);
					rules = Files.readAllLines(Paths.get("/etc/suricata/rules/custom.rules"));
					boolean check_existence = false;
					for (String r : rules ){
						if ((r.contains(targetIP)) && (r.contains("Spoofed IP packet"))){
							check_existence = true;
						}
					}
					if(!check_existence) {
						// custom.rules is a set of user defined rules used by Suricata to detect intrusions (here it's modified to detect packets comming from a spoofed IP)
						Files.write(Paths.get("/etc/suricata/rules/custom.rules"), rule.getBytes(), StandardOpenOption.APPEND);
						//Files.write(Paths.get("/etc/netns/h2/suricata/rules/custom.rules"), rule.getBytes(), StandardOpenOption.APPEND);
						//Files.write(Paths.get("/etc/netns/h1/suricata/rules/custom.rules"), rule.getBytes(), StandardOpenOption.APPEND);

						// Write and register CQELS query to detect spoofed IP packets
						String mitnickQuery = FileManager.get().readWholeFileAsUTF8(CQELSDATA+ "/mitnick_query.cqels");
						mitnickQuery = generateQuery(mitnickQuery,targetIP);
						QueryRouter qrMitnick = context.engine().registerSelectQuery(mitnickQuery);

						qrMitnick.addListener(new SelectListener() {
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
								System.out.println("Possible Mitnick attack ongoing (SYN flood + IP spoofing)");
							}
						});

					}
				}catch (IOException e) {
					e.printStackTrace();
				}

			}
		});



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
		return template.replaceAll("VICTIM", name);
	}
		
	public static  Node n(String st) {
		return Node.createURI(st);
	}
}
