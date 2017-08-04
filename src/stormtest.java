import java.io.*;
import java.net.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import org.json.JSONException;
import org.json.JSONObject;
import org.msgpack.MessagePack;
import org.msgpack.annotation.Message;

import com.fasterxml.jackson.annotation.JsonProperty;



public class stormtest {
	
	
	private int myServerPort = 4343; 
	private String myServerAddr = "52.53.65.44";

	
	
	public static void main(String[] args) {
	    String path = args[0];

		try {
			new stormtest().go(path);
		} catch (Exception e) {
			System.err.println(e);
		}
	}
	
	   
	private void go(String path) throws IOException, SocketException, UnknownHostException, InterruptedException, JSONException, ParseException {

		
		//config file
		
	
		String schoice = readFile(path);
		schoice= schoice.replaceAll("\\n", "\n");

		
		System.out.println(schoice);
		
		JSONObject jsonforproperty = new JSONObject(schoice.toString());
		int jsonlength = jsonforproperty.length();
		
		
		if(jsonforproperty.has("destaddr") && !jsonforproperty.getString("destaddr").equals(null))
		{
			
			myServerAddr = jsonforproperty.getString("destaddr");
			
		}
		
		DatagramSocket dgSocket = new DatagramSocket();
		
		InetAddress inAddr = InetAddress.getByName(myServerAddr);
		
		System.out.println("Enter the file path");
		// System.out.println("for testing purpose enter the string below"); 
		// System.out.println(s);
		Scanner sinput = new Scanner(System.in);
		String Data = sinput.nextLine();

		String strLine;

		FileInputStream fstream = new FileInputStream(Data);
		BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
		List<String> list = new ArrayList<String>();

		
		
		
		  
		
	    
	    
	    
    	MyMessage src = new MyMessage();

	    
		while ((strLine = br.readLine()) != null)   {
		    list.add(strLine);
			String[] stringArr = list.toArray(new String[0]);

		    List<String> names = new LinkedList<String>();
			for (int i = 0; i < stringArr.length; i++)
		{

			   String name = stringArr[i];
			    names =  (Arrays.asList(name.split(" ")));
		  
		       src.setName(names.get(0));
		       src.setTimestamp(Long.parseLong(names.get(1)));
		       src.setValue(Double.parseDouble(names.get(2)));
		      
		       String tagvalue[] = names.get(3).split("[^a-zA-Z1-9]+");
		       

		       
		       
		       List<Object> taglist = new ArrayList<>();
		       taglist.add(tagvalue[1]);
		       taglist.add(tagvalue[2]);
		       
		       
			    
			    String tagstring = taglist.toString().replaceAll("\\[|\\]", "");
			    
			    
			    
			    String finaltags[] = tagstring.split(", ");
			    
			    Map<String, String> finaltagmap = new HashMap<String, String>();
			    
			    finaltagmap.put(finaltags[0], finaltags[1]);
			    
				
				//System.out.println(finaltagmap);
				
		        src.setTags(finaltagmap);
		      // src.timestamp = Long.parseLong(names.get(1));
		        // /Users/amikar/downloads/stormdata.txt
		       
		    }
			//System.out.println(src);

			    
			    if(jsonforproperty.has("debug") && jsonforproperty.getString("debug").equals("true"))
			    {
			    	System.out.println(names);
			    }
			    if(jsonforproperty.has("destport") && !jsonforproperty.getString("destport").equals(null))
			    {
			    	myServerPort = jsonforproperty.getInt("destport");
			    }
			    			
			    
			
		        MessagePack msgpack = new MessagePack();
		        byte[] bytes = msgpack.write(src);
		        
		        
			       
			    DatagramPacket dgPacket = new DatagramPacket(bytes, bytes.length,inAddr, myServerPort);
						
						
								dgSocket.send(dgPacket);
								System.out.println(dgPacket + " Packet sent");
				
				if(jsonforproperty.has("interval") && !jsonforproperty.getString("interval").equals(null))
				{
			        Thread.sleep(jsonforproperty.getInt("interval")*1000);
				}
				else{
					        Thread.sleep(5000);
				}
						    
						    list.remove(strLine);

			}
		    
		  
		

		dgSocket.close();


		

	}

	

	public static String readFile(String filename) {
	    String result = "";
	    try {
	        BufferedReader brt = new BufferedReader(new FileReader(filename));
	        StringBuilder sbt = new StringBuilder();
	        String line = brt.readLine();
	        while (line != null) {
	            sbt.append(line);
	            line = brt.readLine();
	        }
	        result = sbt.toString();
	        brt.close();

	    } catch(Exception e) {
	        e.printStackTrace();
	    
	    }
	    
	    return result;
	}
	
	
}




