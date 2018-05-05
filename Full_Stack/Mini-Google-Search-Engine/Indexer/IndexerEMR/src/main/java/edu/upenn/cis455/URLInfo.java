package edu.upenn.cis455;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

public class URLInfo {

	private String protocal;
	private String hostName;
	private int portNo;
	private String filePath;
	
	/**
	 * Constructor called with raw URL as input - parses URL to obtain host name and file path
	 */
	public URLInfo(String docURL){
		if(docURL == null || docURL.equals(""))
			return;
		docURL = docURL.trim();
		if(!(docURL.startsWith("http://") || docURL.startsWith("https://")) ||docURL.length() < 8)
			return;
		// Stripping off 'http://'
		if(docURL.startsWith("http://")){
			protocal = "http://";
			docURL = docURL.substring(7);			
		}
		if(docURL.startsWith("https://")){		
			protocal = "https://";
			docURL = docURL.substring(8);
		}
			/*If starting with 'www.' , stripping that off too
		if(docURL.startsWith("www."))
			docURL = docURL.substring(4);*/
		int i = 0;
		while(i < docURL.length()){
			char c = docURL.charAt(i);
			if(c == '/')
				break;
			i++;
		}
		String address = docURL.substring(0,i);
		if(i == docURL.length())
			filePath = "/";
		else
			filePath = docURL.substring(i); //starts with '/'
		if(address.equals("/") || address.equals(""))
			return;
		if(address.indexOf(':') != -1){
			String[] comp = address.split(":",2);
			hostName = comp[0].trim();
			try{
				portNo = Integer.parseInt(comp[1].trim());
			}catch(NumberFormatException nfe){
				if(protocal.equals("http://"))
					portNo = 80;
				else
					portNo = 443;
			}
		}else{
			hostName = address;
			if(protocal.equals("http://"))
				portNo = 80;
			else
				portNo = 443;		
		}
	}
	
	public URLInfo(String protocal,String hostName, String filePath){
		this.hostName = hostName;
		this.filePath = filePath;
		this.protocal = protocal;
		if(protocal.equals("http://"))
			this.portNo = 80;
		else
			this.portNo = 443;
	}
	
	public URLInfo(String protocal,String hostName,int portNo,String filePath){
		this.hostName = hostName;
		this.portNo = portNo;
		this.filePath = filePath;
		this.protocal = protocal;
	}
	
	public String getHostName(){
		return hostName;
	}
	
	public void setHostName(String s){
		hostName = s;
	}
	
	public int getPortNo(){
		return portNo;
	}
	
	public void setPortNo(int p){
		portNo = p;
	}
	
	public String getFilePath(){
		return filePath;
	}
	
	public void setFilePath(String fp){
		filePath = fp;
	}

	public String getProtocal(){
		return protocal;
	}

	public static String hash(String text) {
		if (text == null || text.length() == 0) {
			return null;
		}
		try {
			// hash
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			byte[] encodedhash = digest.digest(text.getBytes(StandardCharsets.UTF_8));

			// convert byte to hex
			StringBuffer hexString = new StringBuffer();
			for (int i = 0; i < encodedhash.length; i++) {
				String hex = Integer.toHexString(0xff & encodedhash[i]);
				if(hex.length() == 1) hexString.append('0');
				hexString.append(hex);
			}
			return hexString.toString();
		} catch (Exception e) {
			return null;
		}
	}
}
