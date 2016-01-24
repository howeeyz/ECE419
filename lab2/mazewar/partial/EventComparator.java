/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import java.util.Comparator;
/**
 *
 * @author wingsion
 */
public class EventComparator implements Comparator<MPacket> {
    public int compare(MPacket p1, MPacket p2)
     {
         return p1.sequenceNumber - p2.sequenceNumber;
     }
}
