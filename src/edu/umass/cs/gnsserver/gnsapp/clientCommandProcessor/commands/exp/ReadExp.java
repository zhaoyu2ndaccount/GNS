package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.exp;

import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.exceptions.client.OperationNotSupportedException;
import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnscommon.exceptions.server.FieldNotFoundException;
import edu.umass.cs.gnscommon.exceptions.server.InternalRequestException;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.utils.Format;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.FieldAccess;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.AbstractCommand;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
import edu.umass.cs.gnsserver.utils.JSONUtils;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;

public class ReadExp extends AbstractCommand {
    /**
     *
     * @param module
     */
    public ReadExp(CommandModule module) {
        super(module);
    }

    /**
     *
     * @return the command type
     */
    @Override
    public CommandType getCommandType() {
        return CommandType.ReadExp;
    }


    @Override
    public CommandResponse execute(InternalRequestHeader internalHeader, CommandPacket commandPacket,
                                   ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
            JSONException, NoSuchAlgorithmException, SignatureException, UnsupportedEncodingException, ParseException, InternalRequestException, OperationNotSupportedException, FailedDBOperationException, FieldNotFoundException {
        JSONObject json = commandPacket.getCommand();
        String guid = json.getString(GNSProtocol.GUID.toString());

        assert (internalHeader != null);

        // the opt hair below is for the subclasses... cute, huh?
        String field = json.optString(GNSProtocol.FIELD.toString(), null);

        // Reader can be one of three things:
        // 1) a guid - the guid attempting access
        // 2) the value GNSConfig.GNSC.INTERNAL_OP_SECRET - which means this is a request from another server
        // 3) null (or missing from the JSON) - this is an unsigned read or a mutual auth command
        String reader = json.optString(GNSProtocol.READER.toString(), null);
        // signature and message can be empty for unsigned cases (reader should be null as well)
        String signature = json.optString(GNSProtocol.SIGNATURE.toString(), null);
        String message = json.optString(GNSProtocol.SIGNATUREFULLMESSAGE.toString(), null);
        Date timestamp;
        if (json.has(GNSProtocol.TIMESTAMP.toString())) {
            timestamp = json.has(GNSProtocol.TIMESTAMP.toString())
                    ? Format.parseDateISO8601UTC(json.getString(GNSProtocol.TIMESTAMP.toString())) : null; // can be null on older client
        } else {
            timestamp = null;
        }

        assert(field != null);

        return new CommandResponse(ResponseCode.NO_ERROR, "");
        // return FieldAccess.lookupSingleField(internalHeader, commandPacket, guid, field, reader, signature,
        //        message, timestamp, handler);
    }
}
