package searchengine.CreateHtmlFiles;

import searchengine.store.DisplayUrlObj;
import searchengine.store.UserInfo;

import java.util.List;

public class BasicComponent {
    // create the main page
    public static String createMainPage(String futureSearchType){
        String mainBody = createMainSearch(futureSearchType);
        String loading = createLoading();
        String tab = createTabs(futureSearchType);
        return createTopAndBottm("G09 SearchEngine",tab+mainBody+loading);
    }
    // create the html result
    public static String createFullPage(String title, List<DisplayUrlObj> displayUrlObjs, UserInfo userInfo, String searchType){
        if (searchType != null && searchType.equals("IMG")) {
            String navBar = createNavBar();
            String searchBar = createSearchBar(userInfo.getSearchTime(),userInfo.getQuery(),userInfo.getNewWord());
            StringBuilder sb = new StringBuilder();
            sb.append(displayImgs(displayUrlObjs));
            String resultUnits = sb.toString();
            String pagesNumber = createPagesNumber(userInfo.getTotalDoc(),userInfo.getCurrentPageNum());
            String loading = createLoading();
            String fullPage = createTopAndBottm(title,navBar + searchBar + loading + resultUnits + pagesNumber);
            return fullPage;
        } else {
            String navBar = createNavBar();
            String searchBar = createSearchBar(userInfo.getSearchTime(),userInfo.getQuery(),userInfo.getNewWord());
            StringBuilder sb = new StringBuilder();
            for(DisplayUrlObj d : displayUrlObjs){
                sb.append( createResultunit(d));
            }
            String resultUnits = sb.toString();
            String pagesNumber = createPagesNumber(userInfo.getTotalDoc(),userInfo.getCurrentPageNum());
            String loading = createLoading();
            String fullPage = createTopAndBottm(title,navBar + searchBar + loading + resultUnits + pagesNumber);
            return fullPage;
        }
    }

    // create page not found
    public static String create404Page(){
        String mainBody = notFound();
        return createTopAndBottm("G09 SearchEngine",mainBody);
    }

    private static String createTopAndBottm(String title, String body){
        String html = "<!doctype html>\n" +
                "<html lang=\"en\">\n" +
                "<head>\n" +
                "  <meta charset=\"utf-8\">\n" +
                "  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1, shrink-to-fit=no\">\n" +
                "  <meta name=\"description\" content=\"\">\n" +
                "  <meta name=\"author\" content=\"\">\n" +
                "\n" +
                "  <title>"+title+"</title>\n" +
                "  <!-- Bootstrap core CSS -->\n" +
                "  <link rel=\"stylesheet\" href=\"https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css\">\n" +
                "  <script src=\"https://ajax.googleapis.com/ajax/libs/jquery/3.3.1/jquery.min.js\"></script>\n" +
                "  <script src=\"https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/js/bootstrap.min.js\"></script>\n" +
                "  \n" +
                "</head>" +
                body + "  <footer class=\"text-muted\">\n" +
                "    <div class=\"container\">\n" +
                "      <p class=\"float-right\">\n" +
                "        <a href=\"#\">CopyRight</a>\n" +
                "      </p>\n" +
                "    </div>\n" +
                "  </footer>\n" +
                "</body>\n" +
                "</html>";
        return html;
    }
    private static String createNavBar(){
        String navBar = "  <nav class=\"navbar navbar-inverse navbar-static-top\">\n" +
                "    <div class=\"container\">\n" +
                "      <div class=\"navbar-header\">\n" +
                "        <button type=\"button\" class=\"navbar-toggle collapsed\" data-toggle=\"collapse\" data-target=\"#bs-example-navbar-collapse-1\" aria-expanded=\"false\">\n" +
                "          <span class=\"sr-only\">Toggle navigation</span>\n" +
                "          <span class=\"icon-bar\"></span>\n" +
                "          <span class=\"icon-bar\"></span>\n" +
                "          <span class=\"icon-bar\"></span>\n" +
                "          <span class=\"icon-bar\"></span>\n" +
                "        </button>\n" +
                "        <a class=\"navbar-brand\" href=\"/\">G09 Search Engine</a>\n" +
                "      </div>\n" +
                "    </div>\n" +
                "  </nav>\n";
        return navBar;
    }
    private static String createMainSearch(String futureSearchType){
        String submitForm = createSubmitForm(futureSearchType);

        String result = "    <section class=\"jumbotron text-center\">\n" +
                "      <div class=\"container\">\n" +
                "        <h1>Hey!</h1>\n" +
                "        <h1>What do you want to know?</h1>\n" + submitForm +
                "        <br>\n" +
                "      </div>\n" +
                "    </section>\n";
        String mapApi = createMapApi();
        return result + mapApi;
    }
    private static String createSubmitForm(String futureSearchType){
        // embeded search type
        String added = "";
        if(futureSearchType != null){
            if(futureSearchType.equals("PDF"))
                added = "<input name=\"searchType\" type=\"hidden\" value=\"PDF\">";
            if(futureSearchType.equals("IMG"))
                added = "<input name=\"searchType\" type=\"hidden\" value=\"IMG\">";
        }

        String result =                 "        <form action=\"/search\" method=\"GET\">\n" +
                "          <div class=\"input-group\">\n" +
                "            <input type=\"text\" class=\"form-control\" name=\"query\" placeholder=\"Search\">\n" +
                "            <div class=\"input-group-btn\">\n" +
                "              <button class=\"btn btn-default\" type=\"submit\" onclick=\"show()\" >\n" +
                "                <i class=\"glyphicon glyphicon-search\"></i>\n" +
                "              </button>\n" +
                "            </div>\n" +
                "          </div>\n" +
                "          <div class=\"checkbox\">\n" +
                "            <label><input type=\"checkbox\" onclick=\"displayLocation(39.952219,-75.193214)\"> Find Near </label>" +
                "             <input id=\"invisible-city\" type=\"hidden\" name=\"city\">"+ added +
                "            <label><input type=\"checkbox\" id=\"childproof\" value=\"childproof\" name=\"childproof\"> Child Proof </label>" +
                "             <input id=\"invisible-child\" type=\"hidden\" name=\"childproof\">"+
                "            <label><input type=\"checkbox\" name=\"autocomplete\"> Autocomplete </label>\n" +
                "             <input id=\"invisible-auto\" type=\"hidden\" name=\"autocomplete\">"+
                "          </div>\n" +
                "        </form>\n";
        return result;


    }
    private static String createMapApi(){
        String result = "<script>\n" +
                "  function displayLocation(latitude,longitude){\n" +
                "    var request = new XMLHttpRequest();\n" +
                "    var method = 'GET';\n" +
                "    var url = \"https://maps.googleapis.com/maps/api/geocode/json?latlng=\"+latitude+\",\"+longitude+\"&key=AIzaSyAIMwHWB5pPc2hoK2ZNZp_uvmfleGYZsCM&sensor=true\";\n" +
                "    var async = true;\n" +
                "\n" +
                "    request.open(method, url, async);\n" +
                "    request.onreadystatechange = function(){\n" +
                "      if(request.readyState == 4 && request.status == 200){\n" +
                "        var data = JSON.parse(request.responseText);\n" +
                "        var address = data.results[0];\n" +
                "        document.getElementById(\"invisible-city\").value = address.address_components[3].short_name;\n" +
                "    }\n" +
                "};\n" +
                "request.send();\n" +
                "};\n" +
                "\n" +
                "var successCallback = function(position){\n" +
                "    var x = position.coords.latitude;\n" +
                "    var y = position.coords.longitude;\n" +
                "    console.log(\"x: \" + x);\n" +
                "    console.log(\"y: \" + y);\n" +
                "    displayLocation(x,y);\n" +
                "};\n" +
                "\n" +
                "var errorCallback = function(error){\n" +
                "    var errorMessage = 'Unknown error';\n" +
                "    switch(error.code) {\n" +
                "      case 1:\n" +
                "      errorMessage = 'Permission denied';\n" +
                "      break;\n" +
                "      case 2:\n" +
                "      errorMessage = 'Position unavailable';\n" +
                "      break;\n" +
                "      case 3:\n" +
                "      errorMessage = 'Timeout';\n" +
                "      break;\n" +
                "  }\n" +
                "  document.write(errorMessage);\n" +
                "};\n" +
                "\n" +
                "var options = {\n" +
                "    enableHighAccuracy: true,\n" +
                "    timeout: 10000,\n" +
                "    maximumAge: 0\n" +
                "};\n" +
                "\n" +
                "function findCity(){\n" +
                "    console.log(\"Start call navigator\");\n" +
                "    if (\"geolocation\" in navigator) {\n" +
                "        navigator.geolocation.getCurrentPosition(successCallback,errorCallback,options);        \n" +
                "    }\n" +
                "    else{\n" +
                "        console.log(\"Not enable\");\n" +
                "    }\n" +
                "};\n" +
                "</script>\n" +
                "<script async defer type=\"text/javascript\" src=\"https://maps.google.com/maps/api/js?key=AIzaSyAIMwHWB5pPc2hoK2ZNZp_uvmfleGYZsCM\"></script>\n";
        return result;
    }
    private static String createSearchBar(Double sec,String query, String newWord){
        String timeToSpend = "";
        if(sec != null )
            timeToSpend = "<p class=\"text-center\" \"bg-success\">Search for <mark>" + query + "</mark>Total Search Time: " + sec + " Second</p>";
        String spellCheck = "";
        if(newWord.length() != 0){
//            spellCheck = "<form id=\"my_form\" action=\"/search\" method=\"GET\">\n" +
//                    "      <p class=\"text-center\">Do you mean \n" +
//                    "        <a href=\"javascript:{}\" onclick=\"document.getElementById('my_form').submit(); return false;\">"+newWord+"</a> ?\n" +
//                    "      </p>\n" +
//                    "      <input type=\"hidden\" name=\"query\" value=\""+newWord+"\"> \n" +
//                    "      <input type=\"hidden\" name=\"city\" value=\"\"> \n" +
//                    "</form>\n";
            spellCheck = "<p class=\"text-center\">Do you mean <a href=/search?query="+newWord+"&city= onclick=\"show()\">"+newWord+"</a> ?</p>\n";

            spellCheck += createLoading();
        }
        String mapApi = createMapApi();
        String searchBar = "    <section class=\"text-center\">\n" +
                "      <div class=\"container\">\n" +
                "        <h1>Aren't you good?</h1>\n" +
                "        <form action=\"/search\" method=\"GET\">\n" +
                "          <div class=\"input-group\">\n" +
                "            <input type=\"text\" class=\"form-control\" name=\"query\" placeholder=\"Search\">\n" +
                "            <div class=\"input-group-btn\">\n" +
                "              <button class=\"btn btn-default\" type=\"submit\" onclick=\"show()\" >\n" +
                "                <i class=\"glyphicon glyphicon-search\"></i>\n" +
                "              </button>\n" +
                "            </div>\n" +
                "          </div>\n" +
                "          <div class=\"checkbox\">\n" +
                "            <label><input type=\"checkbox\" onclick=\"displayLocation(39.952219,-75.193214)\" > Find Near </label>" +
                "          <input id=\"invisible-city\" type=\"hidden\" name=\"city\">"+
                "            <label><input type=\"checkbox\" id=\"childproof\" value=\"childproof\" name=\"childproof\"> Child Proof </label>" +
                "             <input id=\"invisible-child\" type=\"hidden\" name=\"childproof\">"+
                "            <label><input type=\"checkbox\" name=\"autocomplete\"> Autocomplete </label>\n" +
                "             <input id=\"invisible-auto\" type=\"hidden\" name=\"autocomplete\">"+
                "          </div>\n" +
                "        </form>\n" +
                "        <br>\n" +
                "      </div>\n" +
                "    </section>\n";


        return searchBar + mapApi + timeToSpend + spellCheck;

    }
    private static String createResultunit(DisplayUrlObj displayUrlObj){

        String result = "    <div class=\" py-5 bg-light\">\n" +
                "      <div class=\"container\">\n" +
                "        <div class=\"row\">\n" +
                "          <div class=\"col-md-9\">\n" +
                "            <h3><a href = \""+displayUrlObj.getUrl()+"\">" +
                "               "+displayUrlObj.getTitle()+"</a><small> "+displayUrlObj.getHost()+
                                "</small></h3>\n" +
                "            <p class=\"text-muted\">"+displayUrlObj.getBriefContent()+"</p>\n" +
                "              <div class=\"btn-group\">\n" +
                "                <button type=\"button\" class=\"btn btn-xs btn-success\">Good</button>\n" +
                "                <button type=\"button\" class=\"btn btn-xs btn-danger\">Bad</button>\n" +
                "              </div>\n" +
                "          </div>\n" +
                "        </div>\n" +
                "      </div>\n" +
                "    </div>\n";
        return result;
    }
    private static String createPagesNumber(int allDocs,int curNum){
        int numOfPg = allDocs/10 + 1;
        numOfPg = Math.min(numOfPg,4);
        StringBuilder sb = new StringBuilder();
        for(int i = 1; i <= numOfPg; i++){
            sb.append("<li><a href=\"/goto?page="+i+"\">"+i+"</a></li>\n" );
        }
        String preNext = "<ul class=\"pager\">\n" +
                "      <li><a href=\"/pre\">Prev</a></li>\n" +
                "      <li><a href=\"/next\">Next</a></li>\n" +
                "    </ul>";
        // if we have first page, we only display next
        if(curNum == 0){
            preNext = "    <ul class=\"pager\">\n" +
                    "      <li><a href=\"/next\">Next</a></li>\n" +
                    "    </ul>\n";
        }
        else if (curNum == numOfPg-1){
            preNext = "    <ul class=\"pager\">\n" +
                    "      <li><a href=\"/pre\">Prev</a></li>\n" +
                    "    </ul>\n";
        }
        String pagesNumber = "    " +
                "    <div class=\"text-center\">\n" +
                "      <ul class=\"pagination \">\n" + sb.toString()+
                "      </ul>\n" +
                "    </div>\n";
        return preNext+pagesNumber;
    }
    private static String displayImgs(List<DisplayUrlObj> displayUrlObjs){
        StringBuilder sb = new StringBuilder();

        int i = 0;
        for(; i < displayUrlObjs.size()/4 && i < displayUrlObjs.size()-3; i+=4) {
            sb.append("<div class=\"row\">\n");
            sb.append("  <div class=\"column\">\n" +
                    "<ul class=\"nav navbar-nav\">"+
                    "<li><a href=\""+displayUrlObjs.get(i).getUrl()+"\">"+displayUrlObjs.get(i).getUrl()+"</a></li>\n"+
                    "<li><a href=\""+displayUrlObjs.get(i+1).getUrl()+"\">"+displayUrlObjs.get(i+1).getUrl()+"</a></li>\n"+
                    "<li><a href=\""+displayUrlObjs.get(i+2).getUrl()+"\">"+displayUrlObjs.get(i+2).getUrl()+"</a></li>\n"+
                    "<li><a href=\""+displayUrlObjs.get(i+3).getUrl()+"\">"+displayUrlObjs.get(i+3).getUrl()+"</a></li>\n"+
                    "</ul>"+
                    "  </div></div>\n");
        }
        sb.append("<div class=\"row\">\n");
        boolean dividedBy4 = true;
        if( i < displayUrlObjs.size()){
            sb.append("  <div class=\"column\">\n");
            dividedBy4 = false;
        }
        while(i < displayUrlObjs.size()){
            sb.append("<li><a href=\""+displayUrlObjs.get(i).getUrl()+"\">"+displayUrlObjs.get(i).getUrl()+"</a></li>\n");
            i++;
        }
        if(!dividedBy4)
            sb.append("</div>\n");
        sb.append("</div>");
        return sb.toString();
    }

    private static String createLoading(){
        String result = "    </section>\n" +
                "              <style>\n" +
                "      .loader {\n" +
                "        border: 16px solid #f3f3f3;\n" +
                "        border-radius: 50%;\n" +
                "        border-top: 16px solid #000500;\n" +
                "        width: 120px;\n" +
                "        height: 120px;\n" +
                "        -webkit-animation: spin 4s linear infinite; /* Safari */\n" +
                "        animation: spin 2s linear infinite;\n" +
                "      }\n" +
                "\n" +
                "      /* Safari */\n" +
                "      @-webkit-keyframes spin {\n" +
                "        0% { -webkit-transform: rotate(0deg); }\n" +
                "        100% { -webkit-transform: rotate(360deg); }\n" +
                "      }\n" +
                "\n" +
                "      @keyframes spin {\n" +
                "        0% { transform: rotate(0deg); }\n" +
                "        100% { transform: rotate(360deg); }\n" +
                "      }\n" +
                "\n" +
                "</style>\n" +
                "\n" +
                "<style>\n" +
                ".center {\n" +
                "    display: none;\n" +
                "    margin-left: auto;\n" +
                "    margin-right: auto; \n" +
                "    width: 10%;\n" +
                "    padding: 10px;\n" +
                "}\n" +
                "</style>\n" +
                "  <div class=\"center\" id=\"c1\">\n" +
                "<div class=\"loader\" id=\"l1\"></div>\n" +
                "</div>\n" +
                "<script>\n" +
                "  function show() {\n" +
                "    document.getElementById(\"c1\").style.display=\"block\";\n" +
                "  }\n" +
                "</script>\n";
        return result;
    }
    private static String notFound(){
        String result = "    <section class=\"jumbotron text-center\">\n" +
                "      <div class=\"container\">\n" +
                "        <h1>404 Page Not Found</h1>\n" +
                "           <style>\n" +
                "      .loader {\n" +
                "        border: 16px solid #f3f3f3;\n" +
                "        border-radius: 50%;\n" +
                "        border-top: 16px solid black;\n" +
                "        border-right: 16px solid black;\n" +
                "        border-bottom: 16px solid black;\n" +
                "        border-left: 16px solid blue;\n" +
                "        width: 120px;\n" +
                "        height: 120px;\n" +
                "        -webkit-animation: spin 4s linear infinite; /* Safari */\n" +
                "        animation: spin 2s linear infinite;\n" +
                "      }\n" +
                "\n" +
                "      /* Safari */\n" +
                "      @-webkit-keyframes spin {\n" +
                "        0% { -webkit-transform: rotate(0deg); }\n" +
                "        100% { -webkit-transform: rotate(360deg); }\n" +
                "      }\n" +
                "\n" +
                "      @keyframes spin {\n" +
                "        0% { transform: rotate(0deg); }\n" +
                "        100% { transform: rotate(360deg); }\n" +
                "      }\n" +
                "\n" +
                "</style>\n" +
                "\n" +
                "<style>\n" +
                ".center {\n" +
                "    display: block;\n" +
                "    margin-left: auto;\n" +
                "    margin-right: auto; \n" +
                "    width: 10%;\n" +
                "    padding: 10px;\n" +
                "}\n" +
                "</style>\n" +
                "  <div class=\"center\" id=\"c1\">\n" +
                "<div class=\"loader\" id=\"l1\"></div>\n" +
                "</div>\n" +
                "        </form>\n" +
                "        <br>\n" +
                "      </div>\n" +
                "    </section>\n";
        return result;
    }
    private String createGoogleMap(){
        return null;
    }

    private static String createTabs(String futureSearchType){
        StringBuilder sb = new StringBuilder();
        sb.append("    <ul class=\"nav nav-tabs\">\n");
        if (futureSearchType != null && futureSearchType.equals("PDF")){
            sb.append("    <li  ><a href=\"/\">Main</a></li>\n" +
                    "        <li class=\"active\"><a href=\"/pdf\">PDF</a></li>\n" +
                    "        <li><a href=\"/img\">Image</a></li>\n" +
                    "    </ul>\n");
        }
        else if (futureSearchType != null &&futureSearchType.equals("IMG")){
            sb.append("<li  ><a href=\"/\">Main</a></li>\n" +
                    "        <li><a href=\"/pdf\">PDF</a></li>\n" +
                    "        <li class=\"active\"><a href=\"/img\">Image</a></li>\n" +
                    "    </ul>");
        }

        else{
            sb.append("    <li  class=\"active\"><a href=\"/\">Main</a></li>\n" +
                    "        <li><a href=\"/pdf\">PDF</a></li>\n" +
                    "        <li><a href=\"/img\">Image</a></li>\n" +
                    "    </ul>\n");
        }
        return sb.toString();
    }
    private static String createGoogleApi(){
        String result = "<!DOCTYPE html>\n" +
                "\n" +
                "<html>\n" +
                "\n" +
                "    <style>\n" +
                "        #map {\n" +
                "            width: 1500px;\n" +
                "            height: 800px;\n" +
                "        }\n" +
                "        html, body {\n" +
                "            height: 100%;\n" +
                "            margin: 0;\n" +
                "            padding: 0;\n" +
                "        }\n" +
                "    </style>\n" +
                "\n" +
                "    <div id=\"map\"></div>\n" +
                "\n" +
                "    <script type=\"text/javascript\" src=\"https://maps.google.com/maps/api/js?key=AIzaSyAIMwHWB5pPc2hoK2ZNZp_uvmfleGYZsCM\"></script>\n" +
                "\n" +
                "    <script>\n" +
                "        if (\"geolocation\" in navigator) {\n" +
                "            navigator.geolocation.getCurrentPosition(function(position) {\n" +
                "                var latlng = new google.maps.LatLng(position.coords.latitude, position.coords.longitude);\n" +
                "                var myOptions = {\n" +
                "                    zoom: 8,\n" +
                "                    center: latlng\n" +
                "                }\n" +
                "                var map = new google.maps.Map(document.getElementById(\"map\"), myOptions);\n" +
                "          });\n" +
                "        } else {\n" +
                "            var para = document.createElement('p');\n" +
                "            para.textContent = 'No geolocation!';\n" +
                "            document.body.appendChild(para);\n" +
                "        }\n" +
                "    </script>\n" +
                "\n" +
                "</html>";
        return result;
    }
}
