package com.miternos.streamset.stage.util;

import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.miternos.streamset.stage.origin.odata.OdataSource;
import microsoft.aspnet.signalr.client.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

import java.lang.reflect.Type;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

public class OdataClient implements Runnable{

    private String deviceNetworkId;

    private String authToken ;

    private String odataUrl;

    private Long resourceRefreshPeriod ;

    private Date startDate;

    private Date endDate ;

    private String resourceType;

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(OdataClient.class);

    private LinkedBlockingQueue<Map> queue = new LinkedBlockingQueue<Map>();

    private List<String> resourceIdList = new ArrayList<>();

    private boolean shutdown = false;


    public OdataClient(String deviceNetworkId, String authToken, String odataUrl, Long resourceRefreshPeriod,Date startDate, String resourceType){
        this.deviceNetworkId=deviceNetworkId;
        this.authToken=authToken;
        this.odataUrl=odataUrl;
        this.resourceRefreshPeriod=resourceRefreshPeriod;
        this.startDate=startDate;
        this.resourceType=resourceType;
    }

    public void disconnect(){
        logger.info("Stopping Odata Client thread");
        shutdown = true ;
    }


    private Logger signalrLogger;

    @Override
    public void run() {


        List<String> result = new ArrayList<String>();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("Authorization", "Bearer " + authToken);
        headers.set("X-DeviceNetwork", deviceNetworkId);

        String requestJson = "";

        logger.debug(requestJson);

        HttpEntity<String> entity = new HttpEntity<String>(requestJson, headers);

        RestTemplate restTemplate = new RestTemplate();



        if ( endDate == null ){
            endDate = new Date(); // First time running, set end date to now !
        } else {                  // Update endDate per refreshPeriod
            startDate = endDate;
            Calendar cal = Calendar.getInstance();
            cal.setTime(endDate);
            cal.add(Calendar.SECOND, resourceRefreshPeriod.intValue());
            endDate = cal.getTime();
        }


        String dateTimeStr = "Datetime";

        String fullOdataUrl =
                odataUrl + "/" + deviceNetworkId + "/" + resourceType + "?$filter=" + dateTimeStr + " gt " + OdataSource.df
                        .format(startDate) + " and " + dateTimeStr + " lt " + OdataSource.df.format(endDate);


        while(true){

            logger.info("Get values from "+fullOdataUrl);

            try {

                ResponseEntity<String> resp = restTemplate.exchange(fullOdataUrl, HttpMethod.GET, entity, String.class);

                JsonObject responseJson = JsonUtil.convertToJson(resp.getBody());

                String values = responseJson.get("value").toString();

                Type type = new TypeToken<ArrayList<HashMap<String,String>>>(){}.getType();

                ArrayList<HashMap<String,String>> listMap = JsonUtil.getGson().fromJson(values,type);

                logger.info("Got "+listMap.size()+" resources, add to queue");

                listMap.forEach(p->{
                    queue.add((Map)p);
                });

                if ( responseJson.get("@odata.nextLink") != null ){
                    fullOdataUrl = URLDecoder.decode(responseJson.get("@odata.nextLink").getAsString(), StandardCharsets.UTF_8.toString());
                } else {
                    break;
                }

            } catch (Exception ex){
                logger.error(ex.getMessage());
                ex.printStackTrace();
                break;
            }

        }


    }





    public Map take() throws InterruptedException {
        return queue.take();
    }
}
