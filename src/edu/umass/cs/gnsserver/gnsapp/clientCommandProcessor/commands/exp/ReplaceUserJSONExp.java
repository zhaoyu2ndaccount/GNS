package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.exp;

import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.gnscommon.utils.Format;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientCommandProcessorConfig;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.FieldAccess;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.MetaDataTypeName;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.UpdateOperation;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AbstractUpdate;
import edu.umass.cs.gnsserver.gnsapp.clientSupport.NSAuthentication;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnsserver.utils.ResultValue;
import edu.umass.cs.utils.Config;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Date;
import java.util.logging.Level;

public class ReplaceUserJSONExp extends AbstractUpdate {

    public ReplaceUserJSONExp(CommandModule module) {
        super(module);
    }

    @Override
    public CommandType getCommandType() {
        return CommandType.ReplaceUserJSONExp;
    }

    @Override
    public UpdateOperation getUpdateOperation() {
        return UpdateOperation.USER_JSON_REPLACE;
    }

    @Override
    public CommandResponse execute(InternalRequestHeader header, CommandPacket commandPacket, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
            JSONException, NoSuchAlgorithmException, SignatureException, ParseException {

        JSONObject json = commandPacket.getCommand();
        String guid = json.getString(GNSProtocol.GUID.toString());
//        String field = json.optString(GNSProtocol.FIELD.toString(), null);
//        String value = json.optString(GNSProtocol.VALUE.toString(), null);
//        String oldValue = json.optString(GNSProtocol.OLD_VALUE.toString(), null);

        JSONObject userJSON = json.has(GNSProtocol.USER_JSON.toString())
                ? new JSONObject(json.getString(GNSProtocol.USER_JSON.toString())) : null;
        // writer might be unspecified so we use the guid
        String writer = json.optString(GNSProtocol.WRITER.toString(), guid);
        String signature = json.optString(GNSProtocol.SIGNATURE.toString(), null);
        String message = json.optString(GNSProtocol.SIGNATUREFULLMESSAGE.toString(), null);
        Date timestamp = json.has(GNSProtocol.TIMESTAMP.toString())
                ? Format.parseDateISO8601UTC(json.getString(GNSProtocol.TIMESTAMP.toString())) : null; // can be null on older client

        // special field for this experiment command
        int idx = json.optInt(GNSProtocol.N.toString(), 0);
        guid = guid+'_'+idx;
        writer = writer + '_'+idx;

        if (json.has("originalBase64")) {
            ClientCommandProcessorConfig.getLogger().log(Level.WARNING,
                    "||||||||||||||||||||||||||| message:{0} original{1}",
                    new Object[]{message, new String(Base64.decode(json.getString("originalBase64")))});
        }

        ResponseCode responseCode = FieldAccess.updateUserJSON(header, commandPacket,
                guid, userJSON, writer, signature, message, timestamp, handler);
        if (!responseCode.isExceptionOrError()) {
            return new CommandResponse(ResponseCode.NO_ERROR, GNSProtocol.OK_RESPONSE.toString());
        } else {
            return new CommandResponse(responseCode, GNSProtocol.BAD_RESPONSE.toString() + " " + responseCode.getProtocolCode());
        }

/*
        try {
            // do the check here, extract the information from the request
            ResponseCode errorCode = NSAuthentication.signatureAndACLCheck(header, guid, field, null,
                    writer, signature, message, MetaDataTypeName.WRITE_WHITELIST, handler.getApp());
            if( errorCode.isExceptionOrError() ) {
                return new CommandResponse(errorCode, GNSProtocol.BAD_RESPONSE.toString() + " " + errorCode.getProtocolCode());
            } else {
                //TODO: insert into db here
                return new CommandResponse(ResponseCode.NO_ERROR, GNSProtocol.OK_RESPONSE.toString());
            }

        } catch (FailedDBOperationException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        // If we are here, that means it is caused by an error of DB operation
        return new CommandResponse(ResponseCode.DATABASE_OPERATION_ERROR, GNSProtocol.BAD_RESPONSE.toString());
*/
    }
}
