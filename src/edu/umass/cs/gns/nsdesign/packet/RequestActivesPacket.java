/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.nsdesign.packet;

import static edu.umass.cs.gns.nsdesign.packet.ActiveNameServerInfoPacket.ACTIVE_NAMESERVERS;
import edu.umass.cs.gns.nsdesign.packet.Packet.PacketType;
import edu.umass.cs.gns.util.Stringifiable;
import edu.umass.cs.gns.util.Util;
import java.net.InetSocketAddress;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Set;

/**
 * This packet is sent by local name server to a name server to request the current active replicas
 * for a name. The name server replies also uses this packet to send the reply to a local name server.
 *
 * If the name server is a replica controller for this name and the name exists, then the field
 * <code>activeNameServers</code> in the packet is filled with the current set of active name servers.
 * Otherwise, <code>activeNameServers</code> is set to null.
 *
 * @author Abhigyan
 */
public class RequestActivesPacket<NodeIDType> extends BasicPacketWithNSAndLNS {

  public static final String NAME = "name";
  public static final String ACTIVES = "actives";
  public static final String LNS_REQ_ID = "lnsreqid";

  /**
   * Name for which the active replicas are being requested
   */
  private final String name;
  
  /**
   * Active name servers for the name. This field is populated when name server
   * sends a reply to a local name server.
   */
  private Set<NodeIDType> activeNameServers = null;

  /**
   * Unique request ID assigned by local name server.
   */
  private final int lnsRequestID;

  public RequestActivesPacket(String name, InetSocketAddress lnsAddress, int lnsRequestID, NodeIDType nameServerID) {
    super(nameServerID, lnsAddress);
    this.name = name;
    this.type = PacketType.REQUEST_ACTIVES;
    this.lnsRequestID = lnsRequestID;
  }

  public RequestActivesPacket(JSONObject json, Stringifiable<NodeIDType> unstringer) throws JSONException {
    super((NodeIDType)json.opt(NAMESERVER_ID),
            json.optString(LNS_ADDRESS, null), json.optInt(LNS_PORT, INVALID_PORT));
    this.name = json.getString(NAME);
    this.activeNameServers = json.has(ACTIVES) ? unstringer.getValuesFromJSONArray(json.getJSONArray(ACTIVES)) : null;
    //this.activeNameServers = json.has(ACTIVES) ? Util.stringToSetOfNodeId(json.getString(ACTIVES)) : null;
    this.type = PacketType.REQUEST_ACTIVES;
    this.lnsRequestID = json.getInt(LNS_REQ_ID);
  }

  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    super.addToJSONObject(json);
    Packet.putPacketType(json, getType());
    json.put(NAME, name);
    if (activeNameServers != null) {
      json.put(ACTIVES, activeNameServers);
      //json.put(ACTIVES, Util.setOfNodeIdToString(activeNameServers));
    }
    json.put(LNS_REQ_ID, lnsRequestID);
    return json;
  }

  public void setActiveNameServers(Set<NodeIDType> activeNameServers) {
    this.activeNameServers = activeNameServers;
  }

  public String getName() {
    return name;
  }

  public Set<NodeIDType> getActiveNameServers() {
    return activeNameServers;
  }

//  public int getLNSID() {
//    return lnsID;
//  }
//  public InetSocketAddress getLnsAddress() {
//    return lnsAddress;
//  }
  public int getLnsRequestID() {
    return lnsRequestID;
  }

//  public NodeIDType getNameServerID() {
//    return nsID;
//  }
}
