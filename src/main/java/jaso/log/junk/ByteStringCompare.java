package jaso.log.junk;

import java.util.Comparator;

import com.google.protobuf.ByteString;

public class ByteStringCompare {

	public static void main(String[] args) {
        // Create two ByteString objects
        ByteString byteString1 = ByteString.copyFromUtf8("abc");
        ByteString byteString2 = ByteString.copyFromUtf8("xyz");
        
        Comparator<ByteString> comparer = ByteString.unsignedLexicographicalComparator();

        // Equality check
        System.out.println("Equality: " + byteString1.equals(byteString2)); // Output: false

        // Lexicographical comparison
        int comparisonResult = comparer.compare(byteString1, byteString2);
        System.out.println("Comparison Result: " + comparisonResult); // Output: Negative value (abc < xyz)

        // Comparing identical ByteStrings
        ByteString byteString3 = ByteString.copyFromUtf8("abc");
        System.out.println("Equality with identical ByteString: " + byteString1.equals(byteString3)); // Output: true
        System.out.println("Comparison with identical ByteString: " + comparer.compare(byteString1, byteString3)); // Output: 0
    }
}