package edu.umass.cs.gnstests;

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.packets.CommandPacket;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.*;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;

public class ExpCommandTest {

    public static void main(String[] args) throws IOException, ClientException, NoSuchAlgorithmException, InvalidKeySpecException {

        String guid = args[0];

        byte[] keyBytes = Files.readAllBytes(Paths.get("conf/exp/private_key.der"));
        PKCS8EncodedKeySpec pri_spec =
                new PKCS8EncodedKeySpec(keyBytes);
        KeyFactory kf = KeyFactory.getInstance("RSA");
        PrivateKey priKey = kf.generatePrivate(pri_spec);


        keyBytes = Files.readAllBytes(Paths.get("conf/exp/public_key.der"));
        X509EncodedKeySpec pub_spec =
                new X509EncodedKeySpec(keyBytes);

        PublicKey pubKey = kf.generatePublic(pub_spec);

        GNSClient client = new GNSClient();
        GuidEntry targetGuid = new GuidEntry("", guid, pubKey, priKey);


        CommandPacket response = client.execute(GNSCommand.fieldUpdateExp(guid, "test", 1, targetGuid, 0));

        System.out.println("Response 1:"+response);

        response = client.execute(GNSCommand.fieldReadExp(guid, "test", targetGuid, 0));

        System.out.println("Response 2:"+response);

        client.close();
    }
}
