/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.utwente.bigdata;

/**
 *
 * @author Richard
 */
public class AssenData {
    // festival start woensdag 24 juni 12:00 (10:00 GMT)
    // 1466762400	1435140000
    public static final int FESTIVAL_START = 1435140000;

    // festival eind zaterdag 27 juni 12:00 (10:00 GMT
    // 1467021600
    public static final int FESTIVAL_STOP = 1435399200;
    
    public static boolean isDuringFestival(int timestamp) {
        return FESTIVAL_START <= timestamp && timestamp <= FESTIVAL_STOP;
    }
}
