package infore.SDE.synopses;

import com.fasterxml.jackson.databind.JsonNode;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;
import org.streaminer.stream.cardinality.CardinalityMergeException;
import org.streaminer.stream.cardinality.HyperLogLogPlus;

public class HyperLogLogPlusSynopsis extends Synopsis {

	HyperLogLogPlus hll;

	public HyperLogLogPlusSynopsis(int uid, String[] parameters) {
	     super(uid, parameters[0],parameters[1],parameters[2]);
	     hll = new HyperLogLogPlus(Integer.parseInt(parameters[3]));
	}

	public HyperLogLogPlusSynopsis(int uid, String[] parameters, HyperLogLogPlus hll) {
		super(uid, parameters[0],parameters[1],parameters[2]);
		this.hll = hll;
	}
		 
	@Override
	public void add(Object k) {
		//ObjectMapper mapper = new ObjectMapper();
		JsonNode node = (JsonNode) k;
		/*try {
			node = mapper.readTree(j);
		} catch (IOException e) {
			e.printStackTrace();
		} */
		String value = node.get(this.valueIndex).asText();
		hll.offer(value);
	}

	@Override
	public Object estimate(Object k) {
		return hll.cardinality();
	}

	@Override
	public Estimation estimate(Request rq) {

		return new Estimation(rq, Double.toString((double)hll.cardinality()), Integer.toString(rq.getUID()));
	}

	@Override
	public Synopsis merge(Synopsis sk) throws CardinalityMergeException {
		return new HyperLogLogPlusSynopsis(this.SynopsisID, new String[]{keyIndex, valueIndex, operationMode},
                (HyperLogLogPlus) hll.merge(((HyperLogLogPlusSynopsis) sk).hll));
	}
		
	}

