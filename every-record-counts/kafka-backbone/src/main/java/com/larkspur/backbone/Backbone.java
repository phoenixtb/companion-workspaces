package com.larkspur.backbone;

import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Part 27: Larkspur's event backbone, from the producer's side.
 *
 * <pre>
 *   pos    --day 2026-08-15 [--abort 3]          POS baskets, one Kafka transaction per 1,000; abort one, then retry it
 *   clicks --day 2026-08-14 --schema v1         clickstream events with schema v1, v2_rename or v2
 *   count  --topic larkspur.pos.basket-events   records per isolation level, and the topic's end offsets
 *   prices [--dir data/lab/part28/prices]       the shelf-price history, one record per price version (Part 28)
 *   upserts --topic larkspur.analytics.top-stores   an upsert topic replayed: latest value per key (Part 28)
 *   live   --day 2026-08-15 --topic T --speed 24 [--limit N]   POS uploads replayed at their own pace (Part 30)
 * </pre>
 * Needs `make up-backbone` (Kafka on localhost:9092, Apicurio Registry on localhost:8085).
 */
public final class Backbone {

    // The Apicurio client logs a production hint about sharing its Vert.x instance on every start. A static field:
    // java.util.logging keeps loggers only weakly, and a collected logger would forget the level.
    private static final Logger APICURIO_CLIENT_LOG = Logger.getLogger("io.apicurio.registry.client.common");

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            throw new IllegalArgumentException("usage: pos|clicks|count|prices|upserts|live [options]");
        }
        APICURIO_CLIENT_LOG.setLevel(Level.SEVERE);
        String[] rest = Arrays.copyOfRange(args, 1, args.length);
        switch (args[0]) {
            case "pos" -> PosProducer.run(Lab.option(rest, "--day", "2026-08-15"),
                    Integer.parseInt(Lab.option(rest, "--abort", "0")));
            case "clicks" -> ClickProducer.run(Lab.option(rest, "--day", "2026-08-14"),
                    Lab.option(rest, "--schema", "v1"));
            case "count" -> TopicCounter.run(Lab.option(rest, "--topic", Lab.POS_TOPIC));
            case "prices" -> PriceProducer.run(Lab.option(rest, "--dir", "data/lab/part28/prices"));
            case "live" -> LiveProducer.run(Lab.option(rest, "--day", "2026-08-15"),
                    Lab.option(rest, "--topic", "larkspur.pos.live"),
                    Double.parseDouble(Lab.option(rest, "--speed", "24")),
                    Integer.parseInt(Lab.option(rest, "--limit", "0")));
            case "upserts" -> UpsertReader.run(Lab.option(rest, "--topic", "larkspur.analytics.top-stores"));
            default -> throw new IllegalArgumentException("unknown command " + args[0]);
        }
    }

    private Backbone() {}
}
