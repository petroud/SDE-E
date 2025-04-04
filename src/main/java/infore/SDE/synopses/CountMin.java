package infore.SDE.synopses;



import com.fasterxml.jackson.databind.JsonNode;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;
import infore.SDE.synopses.Sketches.CM;
import java.io.*;
import java.util.ArrayList;

public class CountMin extends Synopsis implements Serializable {
	private static final long serialVersionUID = 1L;
	private CM cm;
	int count = 0;
	boolean range;
	private CM[] cm_ranges;
	int dyadicRangeBits;

	public CountMin(int uid, String[] parameters) {
		super(uid,parameters[0],parameters[1], parameters[2]);
		if (parameters.length == 7) {
			// Dyadic ranges needed
			range = true;
			dyadicRangeBits = Integer.parseInt(parameters[6]);
			cm_ranges = initialize_ranges(parameters);
		} else {
			// Normal CM
			range = false;
			cm = new CM(Double.parseDouble(parameters[3]),Double.parseDouble(parameters[4]),Integer.parseInt(parameters[5]));
		}

	}

	private CM[] initialize_ranges(String[] parameters) {
		CM[] cm_ranges = new CM[dyadicRangeBits];
		for (int j = 0; j < dyadicRangeBits; j++) {
			// this is cm for range of size 2^j, so first j bits are 1, last for whole range.
			cm_ranges[j] = new CM(Double.parseDouble(parameters[3]),Double.parseDouble(parameters[4]),Integer.parseInt(parameters[5]));
		}
		return cm_ranges;
	}

	private void writeObject(ObjectOutputStream oos) throws IOException {
		oos.defaultWriteObject();

		try {
			// Serialize CM fields
			if (cm != null) {
				byte[] cmBytes = CM.serialize(cm);
				oos.writeObject(cmBytes);
			} else {
				oos.writeObject(null);
			}
			if (cm_ranges != null) {
				for (CM cm_range : cm_ranges) {
					byte[] cmRangeBytes = CM.serialize(cm_range);
					oos.writeObject(cmRangeBytes);
				}
			} else {
				oos.writeObject(null);
			}
		} catch (IOException e) {
			throw new IOException("Failed to serialize CM", e);
		}
	}

	private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		ois.defaultReadObject();

		try {
			// Read serialized CM bytes from input stream
			byte[] cmBytes = (byte[]) ois.readObject();
			// Deserialize CM
			if (range){
				cm_ranges = new CM[dyadicRangeBits];
				for (int j = 0; j < dyadicRangeBits; j++) {
					byte[] cmRangeBytes = (byte[]) ois.readObject();
					if (cmRangeBytes != null) {
						cm_ranges[j] = CM.deserialize(cmRangeBytes);
					} else {
						cm_ranges[j] = null;
					}
				}
			} else {
				cm = CM.deserialize(cmBytes);
			}
		} catch (IOException | ClassNotFoundException e) {
			throw new IOException("Failed to deserialize CM", e);
		}
	}
	 
	@Override
	public void add(Object k) {
		//String j = (String)k;
		count++;
		//ObjectMapper mapper = new ObjectMapper();
		JsonNode node = (JsonNode)k;
        /*try {
            node = mapper.readTree(j);
        } catch (IOException e) {
            e.printStackTrace();
        } */
		long val;
		if (this.valueIndex.startsWith("null")) {
			val = 1;
		} else {
			String value = node.get(this.valueIndex).asText();
			val = (long) Double.parseDouble(value);
		}

		String key = node.get(this.keyIndex).asText();
		if (range) {
			int[][] ranges = rangesToUpdate(key);
			for (int j = 0; j < dyadicRangeBits; j++) {
				// add record to different levels of cm. Logic in CountMin instead of making new cm_range class for parallelization reasons.
				cm_ranges[j].add(ranges[j][0], ranges[j][1], val);
			}
		} else {
			cm.add(key, val);
		}
	}

	private int[][] rangesToUpdate(String key) {
		// We know key is double, because we're using ranges.
		double keyDouble = Double.parseDouble(key);
		int[][] ranges = new int[dyadicRangeBits][2];
		int[] coeff = new int[dyadicRangeBits];

		// Initialize half-point
		int maxInt = (1 << dyadicRangeBits) - 1;
		int halfPoint = (1 << (dyadicRangeBits - 1));

		// Assign first dyadic range correctly
		if (keyDouble < halfPoint) {
			coeff[dyadicRangeBits - 1] = 0;
			ranges[dyadicRangeBits - 1][0] = 0;
			ranges[dyadicRangeBits - 1][1] = halfPoint - 1;
		} else {
			coeff[dyadicRangeBits - 1] = 1;
			ranges[dyadicRangeBits - 1][0] = halfPoint;
			ranges[dyadicRangeBits - 1][1] = maxInt;
		}

		// Compute the dyadic ranges
		int pow = halfPoint;
		for (int j = dyadicRangeBits - 2; j >= 0; j--) {
			pow /= 2;
			coeff[j] = (int) (keyDouble / pow);
			ranges[j][0] = coeff[j] * pow;
			ranges[j][1] = (coeff[j] + 1) * pow - 1;
		}
		return ranges;
	}

	@Override
	public Object estimate(Object k)
	{
		return Long.toString(cm.estimateCount(new Long((long) k)));
	}

	@Override
	public Synopsis merge(Synopsis sk) {
		return sk;	
	}

	@Override
	public Estimation estimate(Request rq) {


		if(rq.getRequestID() % 10 == 6){

			String[] par = rq.getParam();
			par[2]= ""+rq.getUID();
			rq.setUID(Integer.parseInt(par[1]));
			rq.setParam(par);
			rq.setNoOfP(rq.getNoOfP()*Integer.parseInt(par[0]));
			return new Estimation(rq, cm, par[1]);

		}
		String key = rq.getParam()[0];
		String e;
		if (range) {
			ArrayList<int[]> rangesToQuery = rangeToQuery(key);
			double sum = 0;
            for (int[] range : rangesToQuery) {
                sum += cm_ranges[range[0]].estimateCount(range[1], range[2]);
            }
			e = Double.toString(sum);
		} else {
			e = Double.toString((double)cm.estimateCount(key));
		}
		return new Estimation(rq, e, Integer.toString(rq.getUID()));


	}

	private ArrayList<int[]> rangeToQuery(String key) {
		// Key will be in format low-high.
		String[] keys = key.split("-");
		double low = Double.parseDouble(keys[0]);
		double high = Double.parseDouble(keys[1]);
		ArrayList<int[]> temp = new ArrayList<>();
		if (low < 0) {
			low = 0;
		}
		if (high < 0) {
			high = 0;
		}
		double initDiff = high - low + 1;
		long totalSum = 0;
		int pow = 1;
		for (int j = dyadicRangeBits; j >= 0; j--) {
			if (low + pow > high) {
				break;  // Stop if exceeding range
			}
			if (low % (pow * 2) == 0 && low + pow <= high) {
				// Continue increasing the power
			} else {
				int[] range = new int[3];
				range[0] = (int) (Math.log(pow) / Math.log(2));
				range[1] = (int) low;
				range[2] = (int) low + pow - 1;
				totalSum += pow;
				low += pow;  // Move the start forward
				temp.add(range);
			}
			pow *= 2;
		}

		pow = (1 << (dyadicRangeBits - 1)); // Largest dyadic interval possible
		for (int j = dyadicRangeBits - 1; j >= 0; j--) {
			if (low % pow == 0 && low + pow - 1 <= high) {
				int[] range = new int[3];
				range[0] = (int) (Math.log(pow) / Math.log(2));
				range[1] = (int) low;
				range[2] = (int) low + pow - 1;
				totalSum += pow;
				low += pow;
				temp.add(range);
			}
			pow /= 2;  // Reduce the dyadic size
		}
		if (totalSum != initDiff) {
			System.err.println("Error - no full coverage");
		}
		return temp;
	}
}
