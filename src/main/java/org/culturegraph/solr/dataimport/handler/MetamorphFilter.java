package org.culturegraph.solr.dataimport.handler;

import java.util.Optional;

import org.metafacture.framework.ObjectReceiver;
import org.metafacture.framework.StreamReceiver;
import org.metafacture.framework.helpers.DefaultObjectPipe;
import org.metafacture.framework.helpers.DefaultStreamPipe;
import org.metafacture.metamorph.Metamorph;
import org.metafacture.strings.StringConcatenator;

public class MetamorphFilter {

    private DefaultObjectPipe<String, StreamReceiver> decoder;
    private StringConcatenator buf;

    public MetamorphFilter(DefaultObjectPipe<String, StreamReceiver> decoder, Metamorph specification, DefaultStreamPipe<ObjectReceiver<String>> encoder) {
        this.decoder = decoder;
        this.buf = new StringConcatenator();

        decoder.setReceiver(specification)
                .setReceiver(encoder)
                .setReceiver(buf);
    }

    public Optional<String> transform(String record) {
        try {
            decoder.resetStream();
            decoder.process(record);
        } catch (Exception e) {
            return Optional.empty();
        }
        return Optional.of(buf.getString());
    }
}
