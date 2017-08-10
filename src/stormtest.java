import java.io.*;
import java.net.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import org.json.JSONException;
import org.json.JSONObject;
import org.msgpack.MessagePack;
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
List<String> tags = new LinkedList<String>();
List<String> inditags = new LinkedList<String>();



for (int i = 0; i < stringArr.length; i++)
		{
	
	
String name = stringArr[i];
			    names =  (Arrays.asList(name.split(" ")));
		       src.setName(names.get(0));
		       
if(jsonforproperty.has("timestamp") && jsonforproperty.getString("timestamp").equals("data"))
	{		      
		       src.setTimestamp(Long.parseLong(names.get(1)));
	}
else if(jsonforproperty.has("timestamp") && jsonforproperty.getString("timestamp").equals("current"))
{
    src.setTimestamp(new Date().getTime());
}
else{
	src.setTimestamp(Long.parseLong(names.get(1)));
}
	
	src.setValue(Double.parseDouble(names.get(2)));

 tags =(Arrays.asList(name.replaceAll("^[^{]*|[^}]*$", "").split("(?<=\\})[^{]*")));
 
 
 inditags =  (Arrays.asList(tags.get(0).split(" ")));
 
	
	//String stringtag = inditags.toString().replace("[", "")  //remove the commas
	//	    .replace("{", "")  //remove the right bracket
		//    .replace("}", "")
		  //  .replace("]", "")//remove the left bracket
		  //  .trim()  ;
	String[] finalinditags = new String[inditags.size()];
	String new11 = new String();
	String new00 = new String();

	Map<String, String> finaltagmap = new HashMap<String, String>();

	for (int m = 0; m < inditags.size();m++){
	
	finalinditags[m]= inditags.get(m).replace("{", "").replace("}", "").trim();
	String[] new22 = finalinditags[m].split("=");
	
	new11 = new22[0];
	new00 = new22[1];
	
	finaltagmap.put(new11,new00);

	}
	



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
