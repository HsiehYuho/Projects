package crawler.worker.info;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

public class RobotsTxtInfo {

	private HashMap<String,ArrayList<String>> disallowedLinks;
	private HashMap<String,ArrayList<String>> allowedLinks;
	private HashMap<String, Date> lastCrawlTime;
	private HashMap<String,Integer> crawlDelays;
	private ArrayList<String> sitemapLinks;
	private ArrayList<String> userAgents;
	private int defaultDelayInSecond = 5;
	public RobotsTxtInfo(){
		disallowedLinks = new HashMap<String,ArrayList<String>>();
		allowedLinks = new HashMap<String,ArrayList<String>>();
		crawlDelays = new HashMap<String,Integer>();
		sitemapLinks = new ArrayList<String>();
		userAgents = new ArrayList<String>();
		lastCrawlTime = new HashMap<>();
	}

	public void addDisallowedLink(String key, String value){
		if(!disallowedLinks.containsKey(key)){
			ArrayList<String> temp = new ArrayList<String>();
			temp.add(value);
			disallowedLinks.put(key, temp);
		}
		else{
			ArrayList<String> temp = disallowedLinks.get(key);
			if(temp == null)
				temp = new ArrayList<String>();
			temp.add(value);
			disallowedLinks.put(key, temp);
		}
	}

	public void addAllowedLink(String key, String value){
		if(!allowedLinks.containsKey(key)){
			ArrayList<String> temp = new ArrayList<String>();
			temp.add(value);
			allowedLinks.put(key, temp);
		}
		else{
			ArrayList<String> temp = allowedLinks.get(key);
			if(temp == null)
				temp = new ArrayList<String>();
			temp.add(value);
			allowedLinks.put(key, temp);
		}
	}

	public void addCrawlDelay(String key, Integer value){
		crawlDelays.put(key, value);
	}

	public void addSitemapLink(String val){
		sitemapLinks.add(val);
	}

	public void addHost(String key){
		userAgents.add(key);
	}

	public boolean containsHost(String key){
		return userAgents.contains(key);
	}

	public ArrayList<String> getDisallowedLinks(String key){
		return disallowedLinks.get(key);
	}

	public ArrayList<String> getAllowedLinks(String key){
		return allowedLinks.get(key);
	}

	public int getCrawlDelay(String key){
		Integer delay = crawlDelays.get(key);
		if(delay == null)
			delay = defaultDelayInSecond;
		return delay;
	}

	public String print(){
		StringBuilder sb = new StringBuilder();
		for(String userAgent:userAgents){
			sb.append("User-Agent: "+userAgent);
			ArrayList<String> dlinks = disallowedLinks.get(userAgent);
			if(dlinks != null)
				for(String dl:dlinks)
					sb.append("Disallow: "+dl);
			ArrayList<String> alinks = allowedLinks.get(userAgent);
			if(alinks != null)
				for(String al:alinks)
					sb.append("Allow: "+al);
			if(crawlDelays.containsKey(userAgent))
				sb.append("Crawl-Delay: "+crawlDelays.get(userAgent));
			sb.append("\n");
		}
		if(sitemapLinks.size() > 0){
			sb.append("# SiteMap Links");
			for(String sitemap:sitemapLinks)
				sb.append(sitemap);
		}
		return sb.toString();
	}

	public boolean crawlContainAgent(String key){
		return crawlDelays.containsKey(key);
	}
}
