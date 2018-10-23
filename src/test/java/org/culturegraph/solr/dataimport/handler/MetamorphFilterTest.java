package org.culturegraph.solr.dataimport.handler;

import java.util.Optional;

import org.junit.Before;
import org.junit.Test;
import org.metafacture.biblio.marc21.Marc21Decoder;
import org.metafacture.fix.biblio.marc21.Marc21Encoder;
import org.metafacture.metamorph.InlineMorph;
import org.metafacture.metamorph.Metamorph;

import static org.culturegraph.solr.dataimport.handler.FakeMarcRecordFactory.marc21;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class MetamorphFilterTest {

    private MetamorphFilter transformer;
    private Metamorph select003;

    @Before
    public void setUp() {
        select003 = InlineMorph.in(this)
                .with("<rules>")
                .with("<entity name=\"leader\">")
                .with("<data name=\"status\" source=\"leader.status\" />")
                .with("<data name=\"type\" source=\"leader.type\" />")
                .with("<data name=\"bibliographicLevel\" source=\"leader.bibliographicLevel\" />")
                .with("<data name=\"typeOfControl\" source=\"leader.typeOfControl\" />")
                .with("<data name=\"characterCodingScheme\" source=\"leader.characterCodingScheme\" />")
                .with("<data name=\"encodingLevel\" source=\"leader.encodingLevel\" />")
                .with("<data name=\"catalogingForm\" source=\"leader.catalogingForm\" />")
                .with("<data name=\"multipartLevel\" source=\"leader.multipartLevel\" />")
                .with("</entity>")
                .with("<data source=\"003\"/>")
                .with("</rules>")
                .create();
    }

    @Test
    public void shouldContainOnlyField003() {
        transformer = new MetamorphFilter(new Marc21Decoder(), select003, new Marc21Encoder());
        Optional<String> transformation = transformer.transform(marc21("001", "003"));

        assertThat(transformation.isPresent(), is(true));
        assertThat(transformation.get(), is(equalTo(marc21("003"))));
    }

    @Test
    public void shouldContainOnlyField003FromFile() {
        Metamorph select003 = new Metamorph("src/test/resources/select003.xml");
        transformer = new MetamorphFilter(new Marc21Decoder(), select003, new Marc21Encoder());
        Optional<String> transformation = transformer.transform(marc21("001", "003"));

        assertThat(transformation.isPresent(), is(true));
        assertThat(transformation.get(), is(equalTo(marc21("003"))));
    }
}