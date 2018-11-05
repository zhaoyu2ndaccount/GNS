package edu.umass.cs.gnstests;

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.utils.Util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.*;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ExpAccountCreationTest {

    private final static int numGuids = 10;
    private final static int numClients = 10;

    private final static String guidPrefix = "GUID";

    private final static String ACCOUNT = "gaozy@cs.umass.edu";
    private final static String PASSWORD = "";

    private static int rcvd = 0;
    static synchronized void incr() {
        rcvd++;
    }

    static class BatchCreateGuidThread implements Runnable {
        GNSClient client;
        int idx;
        GuidEntry accountGuidEntry;

        BatchCreateGuidThread (GNSClient client, int idx, GuidEntry accountGuidEntry) {
            this.client = client;
            this.idx = idx;
            this.accountGuidEntry = accountGuidEntry;
        }

        @Override
        public void run() {
            Set<String> subGuids = new HashSet<String>();
            for (int j=0; j<numGuids; j++){
                subGuids.add(guidPrefix+(idx*numGuids*100+j));
            }

            long start = System.currentTimeMillis();
            try {
                client.execute(GNSCommand.batchCreateGUIDs(accountGuidEntry, subGuids));
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClientException e) {
                e.printStackTrace();
            }
            System.out.println("Done!\nIt takes " + (System.currentTimeMillis() - start) / 1000.0 + "seconds");
            incr();
        }

    }

    static class CreateAccountThread implements Runnable {
        GNSClient client;
        int idx;

        CreateAccountThread(GNSClient client, int idx) {
            this.client = client;
            this.idx = idx;
        }

        @Override
        public void run() {
            long start = System.currentTimeMillis();
            try {
                client.execute(GNSCommand.createAccount(guidPrefix+"_"+idx));
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClientException e) {
                e.printStackTrace();
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            // System.out.println("Done!\nIt takes " + (System.currentTimeMillis() - start) / 1000.0
            //        + " seconds to create "+idx+"th GUID.");
            incr();
        }
    }

    public static void main(String[] args) throws Exception {
        int num = Integer.parseInt(args[0]);

        ExecutorService executor = Executors.newFixedThreadPool(numClients);

        GNSClient[] clients = new GNSClient[numClients];

        for (int i=0; i<numClients; i++){
            clients[i] = new GNSClient();
        }


        byte[] keyBytes = Files.readAllBytes(Paths.get("conf/exp/private_key.der"));
        PKCS8EncodedKeySpec pri_spec =
                new PKCS8EncodedKeySpec(keyBytes);
        KeyFactory kf = KeyFactory.getInstance("RSA");
        PrivateKey priKey = kf.generatePrivate(pri_spec);


        keyBytes = Files.readAllBytes(Paths.get("conf/exp/public_key.der"));
        X509EncodedKeySpec pub_spec =
                new X509EncodedKeySpec(keyBytes);

        PublicKey pubKey = kf.generatePublic(pub_spec);

        /*
        KeyPair keyPair = KeyPairGenerator.getInstance(GNSProtocol.RSA_ALGORITHM.toString())
                .generateKeyPair();
                */

        GuidEntry guidEntry = new GuidEntry("", "", pubKey, priKey);
        // GuidEntry guidEntry = new GuidEntry("", "", keyPair.getPublic(), keyPair.getPrivate());
        clients[0].execute(GNSCommand.getCommand(CommandType.RegisterAccount, guidEntry,
                GNSProtocol.NAME.toString(), ACCOUNT+100,
                GNSProtocol.PUBLIC_KEY.toString(), KeyPairUtils.publicKeyToBase64ForGuid(guidEntry),
                GNSProtocol.PASSWORD.toString(), "")
                // TODO: decide the actuveSet later
                // GNSProtocol.ACTIVES_SET.toString(), Util.getJSONArray(null))
        );


        /*
        GuidEntry accountGuidEntry = GuidUtils.lookupOrCreateAccountGuid(
                clients[0], ACCOUNT, PASSWORD);

        System.out.println("public key:"+accountGuidEntry.getPublicKeyString()
                +"\n"+"private key:"+accountGuidEntry.getPrivateKey());
                */


        long begin = System.currentTimeMillis();


        while(rcvd < num ) {
            Thread.sleep(500);
            System.out.println("Received:"+rcvd);
        }
        long total = (System.currentTimeMillis() - begin)/1000;
        if (total != 0) System.out.println("It takes "+total+" sec, thruput:"+rcvd*numGuids/total+"/s");

        for (int i=0; i<numClients; i++) {
            clients[i].close();
        }

        executor.shutdown();
    }

}
