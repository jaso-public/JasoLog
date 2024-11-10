package jaso.log.junk;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

public class IpAddressExample {

    public static void main(String[] args) {
        try {
            Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();

            while (networkInterfaces.hasMoreElements()) {
                NetworkInterface networkInterface = networkInterfaces.nextElement();

                // Skip inactive or loopback interfaces
                if (!networkInterface.isUp() || networkInterface.isLoopback()) {
                    continue;
                }

                System.out.println("Interface: " + networkInterface.getDisplayName());

                Enumeration<InetAddress> inetAddresses = networkInterface.getInetAddresses();
                while (inetAddresses.hasMoreElements()) {
                    InetAddress inetAddress = inetAddresses.nextElement();
                    if (!(inetAddress instanceof Inet4Address)) continue;
                    
                    System.out.println("IP Address: " + inetAddress.getHostAddress());
                }
                System.out.println();
            }
        } catch (SocketException e) {
            e.printStackTrace();
        }
    }
}
