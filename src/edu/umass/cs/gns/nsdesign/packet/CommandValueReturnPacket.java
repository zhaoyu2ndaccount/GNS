package edu.umass.cs.gns.nsdesign.packet;

import edu.umass.cs.gns.clientsupport.CommandResponse;
import edu.umass.cs.gns.nsdesign.packet.Packet.PacketType;
import edu.umass.cs.gns.util.NSResponseCode;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Packet format back to the client by local name server in response to a CommandPacket.
 * Contains the original id plus the return value (as a STRING) 
 * plus a possible error code (could be null) 
 * plus instrumentation.
 * 
 * THIS EXACT CLASS IS ALSO IN THE CLIENT so they need to be kept consistent.
 */
public class CommandValueReturnPacket extends BasicPacket {

  private final static String REQUESTID = "reqID";
  private final static String RETURNVALUE = "returnValue";
  private final static String ERRORCODE = "errorCode";
  private final static String LNSROUNDTRIPTIME = "lnsRtt";
  private final static String RESPONDER = "responder";
  private final static String REQUESTCNT = "requestCnt";

  /**
   * Identifier of the request.
   */
  private final int requestId;
  /**
   * The returned value - will most likely become a JSONObject soon
   */
  private final String returnValue;
  /**
   * Indicates if the response is an error.
   */
  private final NSResponseCode errorCode;
  /**
   * The RTT as measured from the LNS out and back.
   */
  private final long LNSRoundTripTime; // how long this query took from the LNS out and back
  /**
   * Instrumentation - what nameserver responded to this query
   */
  private final int responder;
  /**
   * Instrumentation - the request counter from the LNS (can be used to tell how busy LNS is)
   */
  private final long requestCnt;

  /**
   * Creates a CommandValueReturnPacket from a CommandResponse.
   *
   * @param requestId
   * @param response
   * @param requestCnt - current number of requests handled by the LNS (can be used to tell how busy LNS is)
   */
  public CommandValueReturnPacket(int requestId, CommandResponse response, long requestCnt) {
    this.setType(PacketType.COMMAND_RETURN_VALUE);
    this.requestId = requestId;
    this.returnValue = response.getReturnValue();
    this.errorCode = response.getErrorCode();
    this.LNSRoundTripTime = response.getLNSRoundTripTime();
    this.responder = response.getResponder();
    this.requestCnt = requestCnt;
  }

  /**
   * Creates a CommandValueReturnPacket from a JSONObject.
   *
   * @param json
   * @throws JSONException
   */
  public CommandValueReturnPacket(JSONObject json) throws JSONException {
    this.type = Packet.getPacketType(json);
    this.requestId = json.getInt(REQUESTID);
    this.returnValue = json.getString(RETURNVALUE);
    if (json.has(ERRORCODE)) {
      this.errorCode = NSResponseCode.getResponseCode(json.getInt(ERRORCODE));
    } else {
      this.errorCode = NSResponseCode.NO_ERROR;
    }
    this.requestCnt = json.getLong(REQUESTCNT);
    //
    this.LNSRoundTripTime = json.optLong(LNSROUNDTRIPTIME, -1);
    this.responder = json.optInt(RESPONDER, -1);
  }

  /**
   * Converts the command object into a JSONObject.
   *
   * @return
   * @throws org.json.JSONException
   */
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    json.put(REQUESTID, this.requestId);
    json.put(RETURNVALUE, returnValue);
    if (errorCode != null) {
      json.put(ERRORCODE, errorCode.getCodeValue());
    }
    json.put(REQUESTCNT, requestCnt); // instrumentation
    // instrumentation
    if (LNSRoundTripTime != -1) {
      json.put(LNSROUNDTRIPTIME, LNSRoundTripTime);
    }
    // instrumentation
    if (responder != -1) {
      json.put(RESPONDER, responder);
    }
    return json;
  }

  public int getRequestId() {
    return requestId;
  }

  public String getReturnValue() {
    return returnValue;
  }

  public NSResponseCode getErrorCode() {
    return errorCode;
  }

  public long getLNSRoundTripTime() {
    return LNSRoundTripTime;
  }

  public int getResponder() {
    return responder;
  }

  public long getRequestCnt() {
    return requestCnt;
  }

}
