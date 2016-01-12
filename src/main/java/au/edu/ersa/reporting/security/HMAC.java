package au.edu.ersa.reporting.security;

import io.dropwizard.auth.basic.BasicCredentials;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Hex;

import au.edu.ersa.reporting.http.Wrap;

public class HMAC implements AuthAlgorithm {
    private static final String ALGORITHM = "HmacSHA256";

    private final ThreadLocal<Mac> mac;
    private final SecretKeySpec key;

    public HMAC(String hexKey) {
        key = new SecretKeySpec(Wrap.runtimeException(() -> Hex.decodeHex(hexKey.toCharArray())), ALGORITHM);
        mac = ThreadLocal.withInitial(() -> {
            try {
                Mac m = Mac.getInstance(ALGORITHM);
                m.init(key);
                return m;
            } catch (InvalidKeyException | NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public boolean isValid(BasicCredentials credentials, User user) {
        if ((credentials.getPassword() == null) || (user.getSecret() == null)) { return false; }

        return user.getSecret().equalsIgnoreCase(generateSecret(credentials.getPassword()));
    }

    @Override
    public String generateSecret(String plaintext) {
        return Hex.encodeHexString(mac.get().doFinal(plaintext.getBytes()));
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("usage: hexkey plaintext");
            System.exit(1);
        }

        HMAC hmac = new HMAC(args[0]);
        System.out.println(hmac.generateSecret(args[1]));
    }
}
