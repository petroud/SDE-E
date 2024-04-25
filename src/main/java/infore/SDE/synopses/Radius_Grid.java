package infore.SDE.synopses;

import com.fasterxml.jackson.databind.JsonNode;
import infore.SDE.messages.Datapoint;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;

import java.util.ArrayList;
import java.util.HashMap;

public class Radius_Grid extends ContinuousSynopsis{

    //GRID_ID //CELL, LIST OF STREAMS
    private HashMap<Integer, ArrayList<String>> grid = new HashMap<>();
    private Request rq;
    private int count =0;
    public Radius_Grid(int ID, String k, String v) {
        super(ID, k, v);
    }

    public Radius_Grid(Request rq) {
        super(rq.getUID(), rq.getParam()[0], rq.getParam()[1], rq.getParam()[2]);
        this.rq = rq;
    }

    @Override
    public Estimation addEstimate(Object k) {
        count++;
        Datapoint dp = (Datapoint) k;
        JsonNode node = dp.getValues();
        String key = dp.getStreamID();
        String values = node.get("value").asText();
        String[] prices = values.split(";");

        Estimation e;

        //int cell = Integer.parseInt(prices[0])*100000 + Integer.parseInt(prices[1]);
        int cell =(int) (Double.parseDouble(prices[0])*100000 + Double.parseDouble(prices[1]));

        if(count < 100 && cell == 0){
            return null;
        }else if (count > 99 && cell == 0){
            ArrayList<String> dps = new ArrayList<>();
            e = new Estimation(rq, key, dps);
            count =0;
            return e;
        }



       // System.out.println(cell);
        //System.out.println(prices[0]+","+prices[1]);
        if(grid == null){
            grid  = new HashMap<>();
            ArrayList<String> dps = new ArrayList<>();
            e = new Estimation(rq, key, dps);
            dps.add(dp.getStreamID());
            grid.put(cell,dps);
            if(count>100) {
                count=0;
                return e;
            }else{
                return null;
            }

        }else{
            if(grid.containsKey(cell)){
                ArrayList<String> dps = grid.get(cell);
                //System.out.println(dps.size());
                e = new Estimation(rq, key, dps);
                dps.add(dp.getStreamID());
                grid.put(cell,dps);

            }else{
                ArrayList<String> dps = new ArrayList<>();
                e = new Estimation(rq, key, dps);
                dps.add(dp.getStreamID());
                grid.put(cell,dps);
            }
        }
        if(count>100) {
            count=0;
            return e;
        }else{
            return null;
        }
    }


    public ArrayList<Estimation> add_and_provide_estimates(Object k) {
        ArrayList<Estimation> est = new ArrayList<>();
        Datapoint dp = (Datapoint) k;
        JsonNode node = dp.getValues();
        String key = dp.getStreamID();
        String values = node.get("value").asText();
        String[] prices = values.split(";");

        int cell = Integer.parseInt(prices[0])*100000 + Integer.parseInt(prices[1]);
        if(grid == null){
            grid  = new HashMap<>();
            ArrayList<String> dps = new ArrayList<>();
            dps.add(dp.getStreamID());
            grid.put(cell,dps);
            return null;
        }else{
            if(grid.containsKey(cell)){
                ArrayList<String> dps = grid.get(cell);
                dps.add(dp.getStreamID());
                grid.put(cell,dps);
            }else{
                ArrayList<String> dps = new ArrayList<>();
                dps.add(dp.getStreamID());
                grid.put(cell,dps);
            }
        }





        return est;
    }


    @Override
    public void add(Object k) {

    }

    @Override
    public Object estimate(Object k) {
        return null;
    }

    @Override
    public Estimation estimate(Request rq) {
        return null;
    }

    @Override
    public Synopsis merge(Synopsis sk) {
        return null;
    }
}
