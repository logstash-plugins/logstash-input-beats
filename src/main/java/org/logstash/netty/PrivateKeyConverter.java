package org.logstash.netty;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;

import java.io.*;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Security;
import java.security.spec.InvalidKeySpecException;

/*
 * Take an Pem RSA Private key and convert it to a Pkcs8 private key that netty
 * can understand.
 *
 */
public class PrivateKeyConverter {
    static final Logger logger = LogManager.getLogger(PrivateKeyConverter.class.getName());

    private final String passphrase;
    private FileReader file;

    public PrivateKeyConverter(String filepath, String pass) throws FileNotFoundException {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());

        file = new FileReader(filepath);
        passphrase = pass;
    }


    public InputStream convert() throws IOException, InvalidKeySpecException, NoSuchAlgorithmException {
        logger.debug("Converting Private keys if needed");
        PrivateKey kp = loadKeyPair();
        return generatePkcs8(kp);
    }

    private InputStream generatePkcs8(PrivateKey kp) throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
        logger.debug("Generate a Pkcs8 private key: " + kp.getFormat());

        StringWriter out = new StringWriter();
        JcaPEMWriter writer = new JcaPEMWriter(out);
        writer.writeObject(kp);
        writer.close();

        return new ByteArrayInputStream(out.toString().getBytes());
    }

    private PrivateKey loadKeyPair() throws IOException {
        PEMParser reader = new PEMParser(file);
        Object pemObject;

        JcaPEMKeyConverter converter = new JcaPEMKeyConverter().setProvider("BC");

        while((pemObject = reader.readObject()) != null) {
            if(pemObject instanceof PEMKeyPair) {
                PrivateKeyInfo pki = ((PEMKeyPair) pemObject).getPrivateKeyInfo();
                return converter.getPrivateKey(pki);
            }
        }

        return null;
    }

    private boolean hasPassword() {
        if(passphrase != null) {
            return true;
        } else {
            return false;
        }
    }
}
