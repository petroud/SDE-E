package infore.SDE.synopses;

import com.fasterxml.jackson.databind.JsonNode;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;
import org.streaminer.stream.cardinality.CardinalityMergeException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class SpatialSketch extends Synopsis {
    int uid;
    Synopsis[][] sketches;
    int minX;
    int maxX;
    int minY;
    int maxY;
    int maxRes;
    static int resolution = 1;
    int levels;
    int nestedSynopsisID;
    HashMap<Integer, Synopsis[][]> sketch;
    String[] basicSketchParametersArray;
    static ArrayList<Dyadic1D> top_level_intervals;
    boolean reduceMethod; // 0: sum, 1: merge

    public SpatialSketch(int uid, String[] parameters) {
        super(uid, parameters[0], parameters[1], parameters[2]);
        // Format parameters: [KeyIndex, ValueIndex, OperationMode, BasicSketchParameters, SynopsisID, minX, maxX, minY, maxY, maxRes]
        this.uid = uid;
        String basicSketchParameters = parameters[3];
        basicSketchParametersArray = basicSketchParameters.split(";");
        nestedSynopsisID = Integer.parseInt(parameters[4]);
        minX = Integer.parseInt(parameters[5]);
        maxX = Integer.parseInt(parameters[6]);
        minY = Integer.parseInt(parameters[7]);
        maxY = Integer.parseInt(parameters[8]);
        maxRes = Integer.parseInt(parameters[9]);
        // Check if maxRes is power of 2:
        if (!isPowerOfTwo(maxRes)) {
            throw new IllegalArgumentException("Invalid resolution for SpatialSketch: " + maxRes + " (expected power of 2)");
        }
        //resY = Integer.parseInt(parameters[10]);
        sketches = new Synopsis[maxRes][maxRes];
        levels = (int) Math.floor(Math.log(maxRes) / Math.log(2)) + 1;
        sketch = new HashMap<>(levels*levels);
        for (int x_pow = 0; x_pow < levels; x_pow++) {
            for (int y_pow = 0; y_pow < levels; y_pow++) {
                int x_dim = (int) Math.pow(2, x_pow);
                int y_dim = (int) Math.pow(2, y_pow);
                Synopsis[][] sketchArray = null;
                switch (nestedSynopsisID) {
                    case 1:
                        sketchArray = new CountMin[x_dim][y_dim];
                        reduceMethod = false;
                        break;
                    case 2:
                        sketchArray = new Bloomfilter[x_dim][y_dim];
                        reduceMethod = true;
                        break;
                    case 3:
                        sketchArray = new AMSsynopsis[x_dim][y_dim];
                        reduceMethod = true;
                        break;
                    case 4:
                        sketchArray = new MultySynopsisDFT[x_dim][y_dim];
                        reduceMethod = true;
                        break;
                    case 5:
                        // not implemented
                        break;
                    case 6:
                        sketchArray = new FinJoinCoresets[x_dim][y_dim];
                        reduceMethod = true;
                        break;
                    case 7:
                        sketchArray = new HyperLogLogSynopsis[x_dim][y_dim];
                        reduceMethod = true;
                        break;
                    case 8:
                        sketchArray = new StickySamplingSynopsis[x_dim][y_dim];
                        reduceMethod = true;
                        break;
                    case 31:
                        sketchArray = new OmniSketch[x_dim][y_dim];
                        reduceMethod = false;
                        break;
                    default:
                        throw new IllegalArgumentException("Invalid synopsis ID for SpatialSketch: " + nestedSynopsisID + " (expected 1, 2, 3, 4, 5, 6, 7, 8 or 31)");
                }

                assert sketchArray != null;
                for (int i = 0; i < x_dim; i++) {
                    for (int j = 0; j < y_dim; j++) {
                        sketchArray[i][j] = null;
                    }
                }
                sketch.put(dimToIndex(x_dim, y_dim), sketchArray);
            }
        }

        top_level_intervals = new ArrayList<>();
        top_level_intervals.add(new Dyadic1D(1, maxRes, 1));
    }

    private boolean isPowerOfTwo(int n) {
        if (n == 0) {
            return false;
        }
        while (n != 1) {
            if (n % 2 != 0) {
                return false;
            }
            n = n / 2;
        }
        return true;
    }

    private int dimToIndex(int x, int y) {
        return x + y * (maxRes + 1);
    }

    private int getNormX(int x) {
        return getGridIndex(x, minX, maxX, maxRes);
    }
    private int getNormY(int y) {
        return getGridIndex(y, minY, maxY, maxRes);
    }

    private int getGridIndex(int coordinate, int minDomain, int maxDomain,int res) {
        return (int) Math.floor((double) (coordinate - minDomain) / (maxDomain - minDomain) * res);
    }
    int prev_x = -1;
    int prev_y = -1;
    int[][] prev_x_intervals;
    int[][] prev_y_intervals;
    @Override
    public void add(Object k) {
        JsonNode node = (JsonNode)k;
        int x_input = node.get("x").asInt();
        int y_input = node.get("y").asInt();
        if (x_input < minX || x_input >= maxX || y_input < minY || y_input >= maxY) {
            throw new IllegalArgumentException("Invalid coordinates for SpatialSketch: (" + x_input + ", " + y_input + ")");
        }
        int x = getNormX(x_input);
        int y = getNormY(y_input);
        int[][] x_intervals;
        int[][] y_intervals;

        if (x == prev_x) {
            x_intervals = prev_x_intervals;
        } else {
            x_intervals = FindChildIntervals(x + 1, maxRes);
            prev_x_intervals = x_intervals;
            prev_x = x;
        }

        if (y == prev_y) {
            y_intervals = prev_y_intervals;
        } else {
            y_intervals = FindChildIntervals(y + 1, maxRes);
            prev_y_intervals = y_intervals;
            prev_y = y;
        }
        for (int i = 0; i < levels; i++) {
            if (x_intervals[i][0] == -1) {
                break;
            }
            for (int j = 0; j < levels; j++) {
                UpdateInterval(x_intervals[i][0]-1, x_intervals[i][1]-1, y_intervals[j][0]-1, y_intervals[j][1]-1, k);
            }
        }
    }

    private void UpdateInterval(int x1, int x2, int y1, int y2, Object k) {
        int key = dimToIndex(maxRes/(x2 - x1 + 1), maxRes/(y2 - y1 + 1));
        if (!sketch.containsKey(key)) {
            return; // Grid is dropped. No need to update.
        }
        Synopsis[][] sketchArray = sketch.get(key);
        int x_cell = x1/(x2 - x1 + 1);
        int y_cell = y1/(y2 - y1 + 1);
        if (sketchArray[x_cell][y_cell] == null) {
            switch (nestedSynopsisID) {
                case 1:
                    sketchArray[x_cell][y_cell] = new CountMin(uid, basicSketchParametersArray);
                    break;
                case 2:
                    sketchArray[x_cell][y_cell] = new Bloomfilter(uid, basicSketchParametersArray);
                    break;
                case 3:
                    sketchArray[x_cell][y_cell] = new AMSsynopsis(uid, basicSketchParametersArray);
                    break;
                case 4:
                    sketchArray[x_cell][y_cell] = new MultySynopsisDFT(uid, basicSketchParametersArray);
                    break;
                case 6:
                    sketchArray[x_cell][y_cell] = new FinJoinCoresets(uid, basicSketchParametersArray);
                    break;
                case 7:
                    sketchArray[x_cell][y_cell] = new HyperLogLogSynopsis(uid, basicSketchParametersArray);
                    break;
                case 8:
                    sketchArray[x_cell][y_cell] = new StickySamplingSynopsis(uid, basicSketchParametersArray);
                    break;
                case 31:
                    sketchArray[x_cell][y_cell] = new OmniSketch(uid, basicSketchParametersArray);
                    break;
                default:
                    throw new IllegalArgumentException("Invalid synopsis ID for SpatialSketch: " + nestedSynopsisID + " (expected 1, 2, 3, 4, 5, 6, 7, 8 or 31)");
            }
        }
        sketchArray[x_cell][y_cell].add(k);
    }

    private int[][] FindChildIntervals(int target, int int_end) {
        int start = 1;
        int end = int_end;
        int[][] intervals = new int[levels][2];
        for (int i = 0; i < intervals.length; i++) {
            intervals[i] = new int[]{-1, -1};
        }
        int diff = end - start + 1;
        for (int i = 0; i < levels; i++) {
            if (diff < resolution){
                // Check if interval is large enough to be considered for given resolution.
                break;
            }
            intervals[i][0] = start;
            intervals[i][1] = end;
            if (target == start && target == end) {
                // base case (target, target)
                break;
            } else if (start == end) {
                throw new IllegalArgumentException("Invalid interval for SpatialSketch: (" + start + ", " + end + ")");
            } else {
                // split interval
                if (isWithin(target, start, start + diff/2 - 1)) {
                    end = start + diff/2 - 1;
                } else if (isWithin(target, start + diff/2, end)) {
                    start = start + diff/2;
                } else {
                    throw new IllegalArgumentException("Invalid interval for SpatialSketch: (" + start + ", " + end + ")");
                }
                diff = diff/2;
            }
        }
        return intervals;
    }

    private boolean isWithin(int target, int start, int end) {
        return target >= start && target <= end;
    }


    @Override
    public Object estimate(Object k) {

        throw new IllegalArgumentException("Not implemented yet");
    }

    private static class Sum {
        public double sum = 0;
        public Sum() {
        }
        public  void add(double value) {
            sum += value;
        }
        public double getSum() {
            return sum;
        }
    }

    private static class Reduce {
        public double sum = 0;
        Synopsis merged;
        boolean reduceMethod;
        public Reduce(boolean reduceMethod) {
            this.reduceMethod = reduceMethod;
        }
        public  void reduce(double value) {
            sum += value;
        }

        public void reduce(Synopsis syn) throws CardinalityMergeException {
            if (merged == null) {
                merged = syn;
            } else {
                merged = merged.merge(syn);
            }
        }

        public double getSum() {
            return sum;
        }

        public Synopsis getMerged() {
            return merged;
        }

        public String getEstimate(Request nestedRequest) throws CardinalityMergeException {
            if (reduceMethod) {
                if (getMerged() != null) {
                    return getMerged().estimate(nestedRequest).getEstimation().toString();
                } else {
                    return "0";
                }
            } else {
                return Double.toString(getSum());
            }
        }
    }

    @Override
    public Estimation estimate(Request rq) throws CardinalityMergeException {

        // Format: [x1, x2, y1, y2, basiSketchSynId, nestedParameters]
        if(rq.getRequestID() % 10 == 6) {
            throw new UnsupportedOperationException("Not implemented yet");
        }
        int subQueries = 0; // For future use with dynamic spatial sketch.
        Reduce sum;
        sum = new Reduce(reduceMethod);

        String[] parameters = rq.getParam();

        int x1 = getNormX(Integer.parseInt(parameters[0]));
        int x2 = getNormX(Integer.parseInt(parameters[1]));
        int y1 = getNormY(Integer.parseInt(parameters[2]));
        int y2 = getNormY(Integer.parseInt(parameters[3]));

        String[] nestedParameters = new String[parameters.length - 5];
        System.arraycopy(parameters, 5, nestedParameters, 0, nestedParameters.length);

        Request nestedRequest = new Request(rq.getDataSetkey(), rq.getRequestID(),
                rq.getSynopsisID(), rq.getUID(), rq.getStreamID(), nestedParameters, rq.getNoOfP(), rq.getExternalUID());
        // make new datastream and add message to it
        ArrayList<Dyadic2D> dyadic_intervals = GetDyadicIntervals(x1, x2, y1, y2);

        for (Dyadic2D di : dyadic_intervals) {
            di.x1--;
            di.x2--;
            di.y1--;
            di.y2--;
            boolean query_success = QueryDyadicInterval(di, nestedRequest, sum);
            if (!query_success) {
                subQueries += RecurseQueryDyadicInterval(di, nestedRequest, sum);
            } else {
                subQueries++;
            }
        }
        String estimate = sum.getEstimate(nestedRequest);

        return new Estimation(rq, estimate, rq.getUID() + "_" + Arrays.toString(parameters));
    }

    private int RecurseQueryDyadicInterval(Dyadic2D di, Request rq, Reduce sum) throws CardinalityMergeException {
        Dyadic2D di1; Dyadic2D di2;
        di1 = di2 = di;
        int x_dim = di.x2 - di.x1 + 1;
        int y_dim = di.y2 - di.y1 + 1;

        if (x_dim == resolution && y_dim == resolution) {
            return 0; // Reached maximum resolution. No further subQueries.
        } else if (x_dim >= y_dim) {
            di1.x2 = di1.x1 + x_dim/2 - 1;
            di2.x1 = di2.x1 + x_dim/2;
        } else {
            di1.y2 = di1.y1 + y_dim/2 - 1;
            di2.y1 = di2.y1 + y_dim/2;
        }
        int subQueries = 0;
        if (!QueryDyadicInterval(di1, rq, sum)) {
            subQueries += RecurseQueryDyadicInterval(di1, rq, sum);
        } else {
            subQueries++;
        }
        if (!QueryDyadicInterval(di2, rq, sum)) {
            subQueries += RecurseQueryDyadicInterval(di2, rq, sum);
        } else {
            subQueries++;
        }
        return subQueries;

    }

    private boolean QueryDyadicInterval(Dyadic2D di, Request rq, Reduce sum) throws CardinalityMergeException {
        int key = dimToIndex(maxRes/(di.x2 - di.x1 + 1), maxRes/(di.y2 - di.y1 + 1));
        if (sketch.containsKey(key)) {
            int x_cell = di.x1/(di.x2 - di.x1 + 1);
            int y_cell = di.y1/(di.y2 - di.y1 + 1);
            Synopsis[][] sketchArray = sketch.get(key);
            // Grid exists, but cell is empty. Estimate at this dyadic interval is 0.
            if (sketchArray[x_cell][y_cell] != null) {
                if (reduceMethod) {
                    sum.reduce(sketchArray[x_cell][y_cell]);
                } else {
                    Estimation est_i = sketchArray[x_cell][y_cell].estimate(rq);
                    sum.reduce(Double.parseDouble((String) est_i.getEstimation()));
                }
            }
            return true;
        } else {
            return false; // Grid does not exist. Need to query subGrids.
        }

    }

    private static ArrayList<Dyadic2D> GetDyadicIntervals(int x1, int x2, int y1, int y2) {
        ArrayList<Dyadic1D> x_intervals = new ArrayList<>();
        ArrayList<Dyadic1D> y_intervals = new ArrayList<>();

        //Sanity check
        if (x1 > x2 || y1 > y2) {
            throw new IllegalArgumentException("Invalid interval for SpatialSketch: (" + x1 + ", " + x2 + ", " + y1 + ", " + y2 + ")");
        }

        // X
        x_intervals = getDyadic1DS(x1, x2, x_intervals);

        // Y
        y_intervals = getDyadic1DS(y1, y2, y_intervals);

        // Combine intervals to 2D intervals
        ArrayList<Dyadic2D> dyadic_intervals = new ArrayList<>(x_intervals.size() * y_intervals.size());
        for (Dyadic1D x_interval : x_intervals) {
            for (Dyadic1D y_interval : y_intervals) {
                Dyadic2D r = new Dyadic2D(x_interval.start, x_interval.end, y_interval.start, y_interval.end, x_interval.coverage * y_interval.coverage);

                dyadic_intervals.add(r);
            }
        }
        return dyadic_intervals;
    }

    private static ArrayList<Dyadic1D> getDyadic1DS(int i1, int i2, ArrayList<Dyadic1D> intervals) {
        for (Dyadic1D interval : top_level_intervals) {
            int overlap = IntervalOverlap(i1 + 1, i2 + 1, interval.start, interval.end);
            if (overlap == 1) {
                // Exact overlap, so interval is required, but continue loop for potential other intervals.
                intervals.add(interval);
            } else if (overlap == 2) {
                // Interval is fully contained within the query interval,therefore other top level intervals do not have to be considered.
                intervals = ObtainIntervals(new Dyadic1D(i1 + 1, i2 + 1, 1), interval);
                break;
            } else if (overlap == 3) {
                // Lower contained. Upper part is out of range, therefore only lower part is required.
                ArrayList<Dyadic1D> sub_intervals = ObtainIntervals(new Dyadic1D(i1 + 1, i2 + 1, 1), interval);
                intervals.addAll(sub_intervals);
                break;
            } else if (overlap == 4) {
                // Upper overlap
                ArrayList<Dyadic1D> sub_intervals = ObtainIntervals(new Dyadic1D(i1 + 1, i2 + 1, 1), interval);
                intervals.addAll(sub_intervals);
            }
        }
        return intervals;
    }


    private static ArrayList<Dyadic1D> ObtainIntervals(Dyadic1D target, Dyadic1D base) {
        // Check resolution compliance
        if (base.end - base.start + 1 < resolution) {
            return new ArrayList<>();
        } else if (base.start == target.start && base.end== target.end) {
            // If exactly overlap, return
            return new ArrayList<Dyadic1D>(){{add(target);}};
        } else {
            // Recursion
            int power = (int) Math.floor(Math.log(base.end - base.start + 1) / Math.log(2));

            // Split interval
            Dyadic1D lower_base = new Dyadic1D(base.start, base.end - (int) Math.pow(2, power - 1), base.coverage);
            Dyadic1D upper_base = new Dyadic1D(base.start + (int) Math.pow(2, power - 1), base.end, base.coverage);

            ArrayList<Dyadic1D> lower_intervals = new ArrayList<>();
            ArrayList<Dyadic1D> upper_intervals = new ArrayList<>();
            lower_intervals = getDyadic1DS(target, base, lower_base, lower_intervals);
            upper_intervals = getDyadic1DS(target, base, upper_base, upper_intervals);
            lower_intervals.addAll(upper_intervals);
            return lower_intervals;
        }
    }

    private static ArrayList<Dyadic1D> getDyadic1DS(Dyadic1D target, Dyadic1D base, Dyadic1D lower_base, ArrayList<Dyadic1D> intervals) {
        if (IntervalOverlap(target.start, target.end, lower_base.start, lower_base.end) != 0) {
            intervals = ObtainIntervals(new Dyadic1D(Math.max(target.start, lower_base.start), Math.min(target.end, lower_base.end), target.coverage), lower_base);
            if (intervals.isEmpty()) {
                base.coverage = (double) (target.end - target.start + 1) / (double) (base.end - base.start + 1);
                intervals.add(base);
            }
        }
        return intervals;
    }

    // Does interval [start1, end1] overlap with [start2, end2]?
    private static int IntervalOverlap(int start1, int end1, int start2, int end2) {
        if (start1 <= start2 && end1 >= end2) {
            return 1; // Overlaps
        } else if (start1 >= start2 && end1 <= end2) {
            return 2; // 1 fully contained within 2
        } else if (start1 <= start2 && end1 >= start2) {
            return 3; // Lower contained
        } else if (start1 >= start2 && start1 <= end2) {
            return 4; // Upper contained
        } else {
            return 0; // No overlap
        }
    }

    private static class Dyadic2D{
        int x1;
        int x2;
        int y1;
        int y2;
        double coverage;
        public Dyadic2D(int x1, int x2, int y1, int y2, double coverage){
            this.x1 = x1;
            this.x2 = x2;
            this.y1 = y1;
            this.y2 = y2;
            this.coverage = coverage;
        }
    }

    private static class Dyadic1D{
        int start;
        int end;
        double coverage;
        public Dyadic1D(int start, int end, double coverage){
            this.start = start;
            this.end = end;
            this.coverage = coverage;
        }
    }
    @Override
    public Synopsis merge(Synopsis sk) {
               return sk;
    }
}
