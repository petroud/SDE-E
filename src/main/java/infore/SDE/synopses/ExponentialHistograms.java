package infore.SDE.synopses;

import com.fasterxml.jackson.databind.JsonNode;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;
import org.streaminer.stream.cardinality.CardinalityMergeException;

import java.util.ArrayList;

public class ExponentialHistograms extends Synopsis {

    ArrayList<Bucket> buckets;
    int k_;

    public ExponentialHistograms(int ID, String[] parameters) {
        super(ID, parameters[0], parameters[1], parameters[2]);
        double epsilon = Double.parseDouble(parameters[3]);
        k_ = (int) Math.ceil(1/epsilon);

        // Let the buckets be numbered from right to left with the most
        //recent bucket being numbered 1.
        buckets = new ArrayList<Bucket>();

    }

    private static class Bucket {
        int end;
        int start;
        Double count;
        private Bucket(int start, int end, Double count) {
            this.end = end;
            this.start = start;
            this.count = count;
        }
    }

    @Override
    public void add(Object k) {
        JsonNode node = (JsonNode)k;
        int timestamp = Integer.parseInt(node.get(this.keyIndex).asText());
        Double value = Double.parseDouble(node.get(this.valueIndex).asText());

        Bucket newBucket = new Bucket(timestamp, timestamp, value);
        // add the new bucket to the list of buckets at position 0, push the rest
        // of the buckets to the right
        buckets.add(newBucket);

        for (int i = buckets.size() - 1; i > -1; i--) {
            if (verifyCounters((ArrayList<Bucket>) buckets, i)) break;
        }
    }

    @Override
    public Object estimate(Object k) {
        return null;
    }

    @Override
    public Estimation estimate(Request rq) throws CardinalityMergeException {
        double sum = 0;
        for (int i = buckets.size() - 1; i > 0; i--) {
            sum += buckets.get(i).count;
        }
        sum += buckets.get(0).count/2;
        return new Estimation(rq, Double.toString(sum), Integer.toString(rq.getUID()));

    }

    @Override
    public Synopsis merge(Synopsis sk) throws CardinalityMergeException {
        if (sk instanceof ExponentialHistograms) {
            ExponentialHistograms other = (ExponentialHistograms) sk;
            // we need to synchronize the two lists of buckets

            boolean sync = false;
            int curBucketOrg = buckets.size() - 1;
            int curBucketOther = other.buckets.size() - 1;
            ArrayList<Bucket> mergedBuckets = new ArrayList<Bucket>();
            while (curBucketOrg >= 0 || curBucketOther >= 0) {
                if (curBucketOther < 0) {
                    // we have no more buckets in the other list
                    // we can add the rest of the buckets from the origin list
                    Bucket newBucket = new Bucket(buckets.get(curBucketOrg).start, buckets.get(curBucketOrg).end, this.buckets.get(curBucketOrg).count);
                    mergedBuckets.add(newBucket);
                    curBucketOrg--;
                    continue;
                } else if (curBucketOrg < 0) {
                    // we have no more buckets in the origin list
                    // we can add the rest of the buckets from the other list
                    Bucket newBucket = new Bucket(other.buckets.get(curBucketOther).start, other.buckets.get(curBucketOther).end, other.buckets.get(curBucketOther).count);
                    mergedBuckets.add(newBucket);
                    curBucketOther--;
                    continue;
                }

                int time_orgin_end = buckets.get(curBucketOrg).end;
                int time_orgin_start = buckets.get(curBucketOrg).start;
                int time_other_end = other.buckets.get(curBucketOther).end;
                int time_other_start = other.buckets.get(curBucketOther).start;
                if (time_orgin_end < time_other_start) {
                    // the two buckets are not overlapping
                    // we can add the other bucket to the list of buckets
                    Bucket newBucket = new Bucket(time_other_start, time_other_end, other.buckets.get(curBucketOther).count);
                    mergedBuckets.add(newBucket);
                    curBucketOther--;
                } else if (time_orgin_start > time_other_end) {
                    // the two buckets are not overlapping
                    // we can add the origin bucket to the list of buckets
                    Bucket newBucket = new Bucket(time_orgin_start, time_orgin_end, this.buckets.get(curBucketOrg).count);
                    mergedBuckets.add(newBucket);
                    curBucketOrg--;
                } else {
                    // the two buckets are overlapping
                    // we need to merge them
                    Bucket b1 = buckets.get(curBucketOrg);
                    Bucket b2 = other.buckets.get(curBucketOther);
                    b1.count += b2.count;
                    b1.start = Math.min(b1.start, b2.start);
                    b1.end = Math.max(b1.end, b2.end);
                    mergedBuckets.add(b1);
                }
            }
            // now we need to go over list of merged buckets and make sure counts per bucket are < k_
            for (int i = mergedBuckets.size() - 1; i > 0; i--) {
                if (verifyCounters((ArrayList<Bucket>) mergedBuckets, i)) break;
            }
            this.buckets = mergedBuckets;
            return this;
        }
        return null;
    }

    private boolean verifyCounters(ArrayList<Bucket> mergedBuckets, int i) {
        if (mergedBuckets.get(i).count > k_) {
            // merge last two buckets
            Bucket b1 = mergedBuckets.get(mergedBuckets.size() - 1);
            mergedBuckets.remove(mergedBuckets.size() - 1);
            Bucket b2 = mergedBuckets.get(mergedBuckets.size() - 1);
            mergedBuckets.remove(mergedBuckets.size() - 1);
            b1.count += b2.count;
            b1.start = b2.start;
            mergedBuckets.add(b1);
        } else {
            // if the bucket is not too large, we can stop merging
            return true;
        }
        return false;
    }
}
