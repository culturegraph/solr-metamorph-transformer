package org.culturegraph.solr.dataimport.handler;

import java.util.Arrays;

import org.metafacture.fix.biblio.marc21.Marc21Encoder;
import org.metafacture.strings.StringConcatenator;

public class FakeMarcRecordFactory {
    /**
     * Produces a Marc21 record that contains only control fields.
     */
    public static String marc21(String ... controlFields) {
        StringConcatenator buf = new StringConcatenator();

        Marc21Encoder encoder = new Marc21Encoder();
        encoder.setReceiver(buf);
        encoder.startRecord("id");
        encoder.startEntity("leader");
        encoder.literal("status", "n");
        encoder.literal("type", "o");
        encoder.literal("bibliographicLevel", "a");
        encoder.literal("typeOfControl", " ");
        encoder.literal("characterCodingScheme", "a");
        encoder.endEntity();

        Arrays.stream(controlFields).forEach(field -> encoder.literal(field, field));

        encoder.endRecord();
        encoder.closeStream();

        return buf.getString();
    }
}
