package cqelsplus.launch;


import cqelsplus.engine.ExecContext;
import cqelsplus.engine.RDFStream;

import java.io.BufferedReader;
import java.io.FileReader;

public class MQTTStream extends RDFStream implements Runnable{
	String txtFile;
	boolean stop=false;
	long sleep=0;
	String message;

	public MQTTStream(ExecContext context, String uri, String txtFile) {
		super(context, uri);
		this.txtFile=txtFile;
	}

	@Override
	public void stop() {
		stop=true;
	}
	public void setRate(int rate){
		sleep=1000/rate;
	}
	
	public void run() {
			try {
				BufferedReader reader = new BufferedReader(new FileReader(txtFile));
				String strLine;
				while ((strLine = reader.readLine()) != null)   {
					//System.out.println(strLine);
					String[] data=strLine.split(" ");
					if (!strLine.equals(""))
						stream(n(data[0]),n(data[1]),n(data[2]));
				}
				reader.close();


			} catch (Exception e) {
				e.printStackTrace();
			} 
	}

	public void setMessage(String message) {
		this.message = message;
	}
}
