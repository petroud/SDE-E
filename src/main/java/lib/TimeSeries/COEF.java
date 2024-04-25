package lib.TimeSeries;

import org.apache.commons.math3.complex.Complex;

public class COEF {
    private String StreamKey;
    private Complex[] fourierCoefficients;
    //private Date time;

    public COEF(String streamKey, Complex[] fourierCoefficients) {
        StreamKey = streamKey;
        this.fourierCoefficients = fourierCoefficients;
    }

    public String getStreamKey() {
        return StreamKey;
    }

    public Complex[] getFourierCoefficients() {
        return fourierCoefficients;
    }

    public String toString(){
      return fourierCoefficients.toString();
    }
}
