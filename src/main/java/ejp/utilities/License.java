/**
 * Copyright (C) 2006 - present David Bulmore  
 * All Rights Reserved.
 *
 * This file is part of Easy Java Websites.
 *
 * EJW is distributed in the hope that it will be useful, but WITHOUT 
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or 
 * FITNESS FOR A PARTICULAR PURPOSE. See the accompanying license 
 * for more details.
 *
 * You should have received a copy of the license along with EJW; if not, 
 * go to http://www.EasierJava.com and download the latest version.
 */

package ejp.utilities;

//import ejp.Database;
//import ejp.DatabaseManager;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.zip.CRC32;
import javax.crypto.Cipher;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;

/**
 * <p>This class is used to decrypt and verify license keys.
 */

@SuppressWarnings("unchecked")
public final class License
  {
    private static byte[] i_o = { '`', '~', '!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '-', '_', '=', '+',
                                  '?', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e',
                                  '[', '{', ']', '}', '\\', '|', ';', ':', ' ', '\'', '"', ',', '<', '.', '>', '/',
                                  'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K',
                                  'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u',
                                  'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z'};

    private static byte[] o_i = { '#', '$', '%', '^', '&', '`', '~', 'A', 'B', 'H', 'I', 'J', ' ', 'K', 'L', 'M',
                                  'Q', 'R', '!', '@', '+', '[', '{', ']', '}', '\\', '*', 'i', 's', 't', 'u', 'v',
                                  'C', 'D', 'E', 'F', 'G', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'N', 'O', 'P',
                                  'r', '>', '/', '|', ';', ':', '\'', '"', ',', '<', '.', '?', '0', '6', '7', '1',
                                  'w', 'x', 'y', '(', ')', '-', '_', '=', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q',
                                  '2', '3', '4', '5', 'd', 'e', 'f', '8', '9', 'a', 'b', 'c', 'g', 'h', 'z'};

    private Map encryptMap, decryptMap;
    private String matchString, message;
    private boolean licenseValid;
    private Date expiration;
    private int maxNumberTimes, numberTimes;
    private boolean messageWritten;

    public License() { setEncryptDecryptReplacement(i_o, o_i); }

    public License(int maxNumberTimes, String message)
      {
        this.maxNumberTimes = maxNumberTimes; 
        this.message = message;
      }
    
    public License(String key, long checksum) throws LicenseException 
      {
        this();
        
        checkLicense(key, checksum);
      }

    public boolean isLicenseValid() throws LicenseException 
      {
        if (!messageWritten)
          {
            System.out.println(message);
            messageWritten = true;
          }
        
        if (expiration != null)
          if (new Date().before(expiration))
            return true;
          else
            throw new LicenseException("Sorry, your temporary license expired on " + expiration + ".  Please obtain a new license key.  Thank you.");
          
        if (maxNumberTimes > 0)
          if (numberTimes++ < maxNumberTimes)
            return true;
          else
            throw new LicenseException("Sorry, the license maximum has been exceeded.  Please obtain a temporary/permanent license key.  Thank you.");

        return licenseValid;
      }

    public String getMessage() 
      {
        if (message == null)
          return "No License Defined: there are " + (maxNumberTimes - numberTimes) + " iterations left";
            
        return message; 
      }
    
    public String getMatchString() { return matchString; }
    
    public Date getExpiration() { return expiration; }
    
    public String toString() { return getMessage(); }

    private void setEncryptDecryptReplacement(byte[] values, byte[] replacements)
      {
        encryptMap = new HashMap();
        decryptMap = new HashMap();

        for (int i = 0; i < values.length; i++)
          {
            encryptMap.put(new Byte(values[i]),new Byte(replacements[i]));
            decryptMap.put(new Byte(replacements[i]),new Byte(values[i]));
          }
      }
    
    private void checkLicense(String license, long checksum) throws LicenseException
      {
        String password = null;

        try
          {
            password = license.substring(license.length() - 8);
            license = license.substring(0, license.length() - 8);

            if (license == null || password == null)
              throw new LicenseException("Sorry, your license is not valid.  Please obtain a new version and/or license key.  Thank you.");

            if (license.startsWith("T"))
              testTimeLicense(license, password, checksum);
            else if (license.startsWith("P"))
              testPermanentLicense(license, password, checksum);
          }
        catch (LicenseException e)
          {
            throw e;
          }
        catch (Exception e)
          {
            throw new LicenseException("Sorry, your license is not valid.  Please obtain a new version and/or license key.  Thank you.");
          }
      }

    private void testTimeLicense(String license, String password, long checksum) throws LicenseException
      {
        String decrypt_str = new String(encryptDecrypt(hexToByte(license.substring(1)), password, false));
        StringTokenizer strtok = new StringTokenizer(decrypt_str,"|");
        String matchString = strtok.nextToken();
        
        if (!checkMatch(matchString, checksum))
          throw new LicenseException("Sorry, your license is not valid.  Please obtain a new version and/or license key.  Thank you.");
        
        expiration = new Date(Long.parseLong(strtok.nextToken(), Character.MAX_RADIX));

        if (!new Date().before(expiration))
          throw new LicenseException("Sorry, your temporary license expired on " + expiration + ", please obtain a new license key.  Thank you.");

        licenseValid = true;
        this.matchString = matchString;
        message = "License is valid.  However it expires on " + expiration + ".  Thank you!";
      }

    private void testPermanentLicense(String license, String password, long checksum) throws LicenseException
      {
        String decrypt_str = new String(encryptDecrypt(hexToByte(license.substring(1)), password, false));
        
        if (!checkMatch(decrypt_str, checksum))
          throw new LicenseException("Sorry, your license is not valid.  Please obtain a new version and/or license key.  Thank you.");
        
        licenseValid = true;
        matchString = decrypt_str;
        message = "License is valid";
      }

    private boolean checkMatch(String match1, long checksum)
      {
        return generateCheckSum(match1) == checksum;
      }
    
    private byte[] hexToByte(String hexString)
      {
        char c[] = hexString.toCharArray();
        int cnt = 0;
        
        for (int i = 0; i < c.length; i++)
          if (c[i] == '-')
            cnt++;

        byte val[] = new byte[(hexString.length() - cnt) / 2];
        int i = 0;

        while (hexString.length() > 0)
          {
            boolean minus = hexString.charAt(0) == '-';
            if (minus)
              hexString = hexString.substring(1);

            String hex_val = hexString.substring(0,2);

            int int_val = Integer.decode("0x" + hex_val).intValue();

            if (int_val > 127 || minus)
              int_val = -(int_val ^ 0x80);

            val[i] = (byte)int_val;

            hexString = hexString.substring(2);
            i++;
          }

        return val;
      }

    private byte[] simpleEncryptDecrypt(byte[] in, boolean encrypt)
      {
        byte out[] = new byte[in.length];
      
        for (int i = 0; i < in.length; i++)
          {
            in[i] = (byte)(in[i] ^ i);
          
            Byte byteVal = (Byte)(encrypt ? encryptMap.get(new Byte(in[i])) : decryptMap.get(new Byte(in[i])));

            if (byteVal != null)
              out[i] = byteVal.byteValue();
            else
              out[i] = in[i];

            out[i] = (byte)(out[i] ^ i);
          }
        
        return out;
      }

    private byte[] encryptDecrypt(byte[] in, String password, boolean encrypt) throws LicenseException
      {
        if (password.length() < 8)
          throw new LicenseException("Password must be 8 characters in length.");

        try
          {
            Cipher c = Cipher.getInstance("PBEWithMD5AndDES");

            c.init(encrypt ? Cipher.ENCRYPT_MODE : Cipher.DECRYPT_MODE, 
                   SecretKeyFactory.getInstance("PBEWithMD5AndDES").generateSecret(new PBEKeySpec(password.toCharArray())), 
                   new PBEParameterSpec(password.getBytes(), 20));

            return encrypt ? c.doFinal(simpleEncryptDecrypt(in, true)) : simpleEncryptDecrypt(c.doFinal(in), false);
          }
        catch (Exception e)
          {
            throw new LicenseException(e);
          }
      }
    
    /**
     * Generates a time based license key that will expire after a defined number of weeks.
     *
     * @param time number of weeks or milliseconds (if time > 1000) before license expires
     * @param match First match string
     * @param password a suitable password (8 characters) for encryption/decryption of the license key
     *
     * @return a license key
     */

    public String generateTimeKey(int time, String match, String password) throws LicenseException
      {
        GregorianCalendar gc = new GregorianCalendar();
        
        gc.add(time > 1000 ? Calendar.MILLISECOND : Calendar.WEEK_OF_YEAR, time);

        byte enc[] = encryptDecrypt(new String(match + "|" + Long.toString(gc.getTimeInMillis(), Character.MAX_RADIX)).getBytes(), password, true);

        return "T" + byteToHex(enc).toUpperCase() + password;
      }

    /**
     * Generates a permanent license key that will never expire.
     *
     * @param match First match string
     * @param password a suitable password (8 characters) for encryption/decryption of the license key
     *
     * @return a license key
     */

    public String generatePermanentKey(String match, String password) throws LicenseException
      {
        byte enc[] = encryptDecrypt(match.getBytes(), password, true);

        return "P" + byteToHex(enc).toUpperCase() + password;
      }

    private String byteToHex(byte[] byteArray)
      {
        StringBuilder hexStrBuf = new StringBuilder();
        
        for (int i = 0; i < byteArray.length; i++)
          {
            int ival = Math.abs(new Integer(byteArray[i]).intValue());
            
            if (byteArray[i] < 0)
              ival = ival ^ 0x80;

            String hex = Integer.toHexString(ival);

            while (hex.length() < 2)
              hex = '0' + hex;

            if (byteArray[i] == -128)
              hexStrBuf.append('-');

            hexStrBuf.append(hex);
          }
        
        return hexStrBuf.toString();
      }
    
    /**
     * Returns a randomly generated password suitable for generating license keys.
     *
     * @return a randomly generated password
     */

    public String generatePassword()
      {
        String password = "";
        
        for (int i = 0; i < 8; i++)
          password += randomCharGenerator();
        
        return password;
      }
    
    public static Long generateCheckSum(String value)
      {
        CRC32 crc = new CRC32();
        crc.update(value.getBytes());
        
        return crc.getValue();
      }
    
    private char randomCharGenerator()
      {
        int r;
        
        do
          {
            r = (int)Math.round(Math.random() * 128);
          }
        while (!(Character.isDigit((char)r) || Character.isUpperCase((char)r)));
        
        return (char)r;
      }

    public static void main(String[] args) throws IOException, LicenseException
      {
        License license = new License();

        if (args.length > 0)
          {
            license.checkLicense(args[0], Long.valueOf(args[1]));

            System.out.println(license);
          }
        else
          {
            //Database db = null;
            BufferedReader b = new BufferedReader(new InputStreamReader(System.in));
            String r_key = null, match = null;//, company = null, project = null;
            int num_weeks = 0;

            //try
            //  {
                //db = new DatabaseManager("db",1,"com.mysql.jdbc.Driver","jdbc:mysql://localhost/licensing","licensor","rosnecil").getDatabase();

                //System.out.println("Enter Company: ");
                //company = b.readLine();

                //System.out.println("Enter Project: ");
                //project = b.readLine();

                System.out.println("Enter Match String: ");
                match = b.readLine();
                
                System.out.println("Checksum = " + license.generateCheckSum(match));

                System.out.println("Permanent Key (y/n): ");

                if (b.readLine().toLowerCase().equals("n"))
                  {
                    System.out.println("Enter Time (int): ");
                    num_weeks = new Integer(b.readLine()).intValue();

                    System.out.println("Time Key = " + (r_key = license.generateTimeKey(num_weeks, match, license.generatePassword())) + "\n");
                  }
                else
                  {
                    System.out.println("Permanent Key = " + (r_key = license.generatePermanentKey(match, license.generatePassword())) + "\n");
                  }

                license = new License(r_key, generateCheckSum(match));
                System.out.println("License - " + license);
                System.out.println("License Match String - " + license.getMatchString());
                
                //db.preparedUpdate("insert into licenses (license, match, checksum, company, project) values(?,?,?,?,?)",
                //                  new Object[] {r_key, match, generateCheckSum(match), company, project});
    /*
              }
            finally
              {
                if (db != null)
                  try
                    {
                      db.close();
                    }
                  catch (Exception e)
                    {
                      e.printStackTrace();
                    }
              }
     */
          }
      }
  }

