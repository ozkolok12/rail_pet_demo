-- STAGING AREA
-- Cleaning staging tables

-- 1. Load without ID
INSERT INTO staging.temp_table_dublicates
SELECT * FROM staging.temp_table;

-- 2. Deleting duplicates
DELETE FROM staging.temp_table_dublicates
WHERE ctid IN (
    SELECT ctid
    FROM (
             SELECT
                 ctid,
                 ROW_NUMBER() OVER (
                     PARTITION BY
                         shipment_date,
                         wagon_number,
                         container_number,
                         document_number,
                         shipment_category,
                         cargo_class,
                         estimated_arrival_date,
                         tonnage_type,
                         arrival_date,
                         release_date,
                         transport_type,
                         cargo_code,
                         cargo_name,
                         departure_country,
                         departure_station_sng,
                         departure_station_sng_code,
                         departure_region,
                         departure_railway,
                         departure_station_rus,
                         departure_station_rus_code,
                         sender,
                         sender_okpo,
                         destination_country,
                         destination_region,
                         destination_railway,
                         destination_station_rus,
                         destination_station_rus_code,
                         destination_station_sng,
                         destination_station_sng_code,
                         receiver,
                         receiver_okpo,
                         wagon_type_group,
                         wagon_type,
                         payer,
                         wagon_owner,
                         renter,
                         operator,
                         wagon_km,
                         volume,
                         tariff,
                         load_dttm,
                         source_file_name
                     ORDER BY stg_id
                     ) AS rn
             FROM staging.temp_table_dublicates
         ) sub
    WHERE rn > 1
);

-- 3. Assigning new id's, without duplicates
WITH numbered AS (
    SELECT
        ctid,
        ROW_NUMBER() OVER (
            ORDER BY
                shipment_date,
                wagon_number,
                container_number,
                document_number,
                shipment_category,
                cargo_class,
                estimated_arrival_date,
                tonnage_type,
                arrival_date,
                release_date,
                transport_type,
                cargo_code,
                cargo_name,
                departure_country,
                departure_station_sng,
                departure_station_sng_code,
                departure_region,
                departure_railway,
                departure_station_rus,
                departure_station_rus_code,
                sender,
                sender_okpo,
                destination_country,
                destination_region,
                destination_railway,
                destination_station_rus,
                destination_station_rus_code,
                destination_station_sng,
                destination_station_sng_code,
                receiver,
                receiver_okpo,
                wagon_type_group,
                wagon_type,
                payer,
                wagon_owner,
                renter,
                operator,
                wagon_km,
                volume,
                tariff,
                load_dttm,
                source_file_name
            ) AS new_id
    FROM staging.temp_table_dublicates
)
UPDATE staging.temp_table_dublicates t
SET stg_id = n.new_id
FROM numbered n
WHERE t.ctid = n.ctid;


-- Insert data to dimensions == from temp_table_dublicates --> staging.temp_table_*

-- > temp_table_wagon
TRUNCATE staging.temp_table_wagon RESTART IDENTITY;
INSERT INTO staging.temp_table_wagon (
        wagon_type_group,
        wagon_type,
        wagon_owner,
        renter,
        operator,
        load_dttm,
        source_file_name
)
SELECT DISTINCT
    wagon_type_group,
    wagon_type,
    wagon_owner,
    renter,
    operator,
    load_dttm,
    source_file_name
FROM staging.temp_table_dublicates;

--> temp_table_sender
TRUNCATE staging.temp_table_sender RESTART IDENTITY;
INSERT INTO staging.temp_table_sender (
    departure_station_sng_code,
    departure_railway,
    departure_country,
    departure_station_sng,
    departure_region,
    departure_station_rus,
    departure_station_rus_code,
    sender,
    sender_okpo,
    payer,
    load_dttm,
    source_file_name
)
SELECT DISTINCT
    departure_station_sng_code,
    departure_railway,
    departure_country,
    departure_station_sng,
    departure_region,
    departure_station_rus,
    departure_station_rus_code,
    sender,
    sender_okpo,
    payer,
    load_dttm,
    source_file_name
FROM staging.temp_table_dublicates;

--> temp_table_consignee
TRUNCATE staging.temp_table_consignee RESTART IDENTITY;
INSERT INTO staging.temp_table_consignee (
    destination_country,
    destination_region,
    destination_railway,
    destination_station_rus,
    destination_station_rus_code,
    destination_station_sng,
    destination_station_sng_code,
    receiver,
    receiver_okpo,
    load_dttm,
    source_file_name
)
SELECT DISTINCT
    destination_country,
    destination_region,
    destination_railway,
    destination_station_rus,
    destination_station_rus_code,
    destination_station_sng,
    destination_station_sng_code,
    receiver,
    receiver_okpo,
    load_dttm,
    source_file_name
FROM staging.temp_table_dublicates;

--> temp_table_cargo
TRUNCATE staging.temp_table_cargo RESTART IDENTITY;
INSERT INTO staging.temp_table_cargo (
    cargo_code,
    cargo_name,
    load_dttm,
    source_file_name
)
SELECT DISTINCT
    cargo_code,
    cargo_name,
    load_dttm,
    source_file_name
FROM staging.temp_table_dublicates;


-- UPSERT STAGING TO DIMENSIONS
-- staging.temp_table_wagon --> dwh.d_wagon
INSERT INTO dwh.d_wagon (
    wagon_type_group,
    wagon_type,
    wagon_owner,
    renter,
    operator,
    load_dttm,
    source_file_name
)
SELECT DISTINCT ON (wagon_type, wagon_owner, renter, operator)
    wagon_type_group,
    wagon_type,
    wagon_owner,
    renter,
    operator,
    load_dttm,
    source_file_name
FROM staging.temp_table_wagon
ORDER BY
    wagon_type,
    wagon_owner,
    renter,
    operator,
    load_dttm DESC
ON CONFLICT (
    wagon_type,
    wagon_owner,
    renter,
    operator
    )
    DO UPDATE
    SET
        wagon_type_group = EXCLUDED.wagon_type_group,
        load_dttm = EXCLUDED.load_dttm,
        source_file_name = EXCLUDED.source_file_name
WHERE dwh.d_wagon.load_dttm < EXCLUDED.load_dttm;


-- staging.temp_table_sender --> dwh.d_sender
INSERT INTO dwh.d_sender (
    departure_station_sng_code,
    departure_railway,
    departure_country,
    departure_station_sng,
    departure_region,
    departure_station_rus,
    departure_station_rus_code,
    sender,
    sender_okpo,
    payer,
    load_dttm,
    source_file_name
)
SELECT DISTINCT ON (
        departure_country,
        sender,
        sender_okpo,
        payer,
        departure_station_rus,
        departure_station_sng
        )
    departure_station_sng_code,
    departure_railway,
    departure_country,
    departure_station_sng,
    departure_region,
    departure_station_rus,
    departure_station_rus_code,
    sender,
    sender_okpo,
    payer,
    load_dttm,
    source_file_name
FROM staging.temp_table_sender
ORDER BY
    departure_country,
    sender,
    sender_okpo,
    payer,
    departure_station_rus,
    departure_station_sng,
    load_dttm DESC
ON CONFLICT (departure_country, sender, sender_okpo, payer, departure_station_rus, departure_station_sng)
    DO UPDATE
    SET
        departure_station_sng_code = EXCLUDED.departure_station_sng_code,
        departure_railway = EXCLUDED.departure_railway,
        departure_country = EXCLUDED.departure_country,
        departure_station_sng = EXCLUDED.departure_station_sng,
        departure_region = EXCLUDED.departure_region,
        departure_station_rus = EXCLUDED.departure_station_rus,
        departure_station_rus_code = EXCLUDED.departure_station_rus_code,
        load_dttm = EXCLUDED.load_dttm,
        source_file_name = EXCLUDED.source_file_name
WHERE dwh.d_sender.load_dttm < EXCLUDED.load_dttm;

-- staging.temp_table_consignee --> dwh.d_consignee
INSERT INTO dwh.d_consignee (
    destination_country,
    destination_region,
    destination_railway,
    destination_station_rus,
    destination_station_rus_code,
    destination_station_sng,
    destination_station_sng_code,
    receiver,
    receiver_okpo,
    load_dttm,
    source_file_name
)
SELECT DISTINCT ON (
    destination_country,
    receiver,
    receiver_okpo,
    destination_station_rus,
    destination_station_sng
    )
    destination_country,
    destination_region,
    destination_railway,
    destination_station_rus,
    destination_station_rus_code,
    destination_station_sng,
    destination_station_sng_code,
    receiver,
    receiver_okpo,
    load_dttm,
    source_file_name
FROM staging.temp_table_consignee
ORDER BY
    destination_country,
    receiver,
    receiver_okpo,
    destination_station_rus,
    destination_station_sng,
    load_dttm DESC
ON CONFLICT (
    destination_country,
    receiver,
    receiver_okpo,
    destination_station_rus,
    destination_station_sng
    )
    DO UPDATE
    SET
        destination_country = EXCLUDED.destination_country,
        destination_region = EXCLUDED.destination_region,
        destination_railway = EXCLUDED.destination_railway,
        destination_station_rus = EXCLUDED.destination_station_rus,
        destination_station_rus_code = EXCLUDED.destination_station_rus_code,
        destination_station_sng = EXCLUDED.destination_station_sng,
        destination_station_sng_code = EXCLUDED.destination_station_sng_code,
        load_dttm = EXCLUDED.load_dttm,
        source_file_name = EXCLUDED.source_file_name
WHERE dwh.d_consignee.load_dttm < EXCLUDED.load_dttm;

-- staging.temp_table_cargo --> dwh.d_cargo
MERGE INTO dwh.d_cargo AS d
USING (
    SELECT DISTINCT ON (cargo_code)
    cargo_code,
    cargo_name,
    load_dttm,
    source_file_name
 FROM staging.temp_table_cargo
 ORDER BY cargo_code, load_dttm DESC
) AS s
ON d.cargo_code = s.cargo_code

WHEN MATCHED AND d.load_dttm < s.load_dttm THEN
    UPDATE SET
        cargo_code = s.cargo_code,
        cargo_name = s.cargo_name,
        load_dttm = s.load_dttm,
        source_file_name = s.source_file_name

WHEN NOT MATCHED THEN
    INSERT (
        cargo_code,
        cargo_name,
        load_dttm,
        source_file_name
    )
    VALUES (
        s.cargo_code,
        s.cargo_name,
        s.load_dttm,
        s.source_file_name
    );

-- staging.temp_table_duplicates --> dwh.f_cargo_shipments
INSERT INTO dwh.f_cargo_shipments (
    shipment_date,
    wagon_number,
    container_number,
    document_number,
    shipment_category,
    cargo_class,
    estimated_arrival_date,
    arrival_date,
    release_date,
    transport_type,
    cargo_id,
    sender_id,
    wagon_id,
    consignee_id,
    wagon_km,
    volume,
    tariff,
    load_dttm,
    source_file_name
)
SELECT DISTINCT ON (
    t.shipment_date,
    t.wagon_number,
    t.container_number,
    t.document_number,
    t.volume,
    t.tariff
    )
    t.shipment_date,
    t.wagon_number,
    t.container_number,
    t.document_number,
    t.shipment_category,
    t.cargo_class,
    t.estimated_arrival_date,
    t.arrival_date,
    t.release_date,
    t.transport_type,

    c.id AS cargo_id,
    s.id AS sender_id,
    w.id AS wagon_id,
    con.id AS consignee_id,

    t.wagon_km,
    t.volume,
    t.tariff,

    t.load_dttm,
    t.source_file_name
FROM staging.temp_table_dublicates t
         LEFT JOIN dwh.d_wagon w
                   ON w.wagon_type  IS NOT DISTINCT FROM t.wagon_type
                       AND w.wagon_owner IS NOT DISTINCT FROM t.wagon_owner
                       AND w.operator    IS NOT DISTINCT FROM t.operator
                       AND w.renter      IS NOT DISTINCT FROM t.renter
         LEFT JOIN dwh.d_sender s
                   ON s.departure_country = t.departure_country
                       AND s.sender IS NOT DISTINCT FROM t.sender
                       AND s.departure_station_sng_code = t.departure_station_sng_code
                       AND s.sender_okpo = t.sender_okpo
                       AND s.departure_station_rus_code = t.departure_station_rus_code
                       AND s.payer = t.payer
         LEFT JOIN dwh.d_consignee con
                   ON con.receiver_okpo = t.receiver_okpo
                       AND con.destination_station_rus_code = t.destination_station_rus_code
                       AND con.destination_station_sng_code = t.destination_station_sng_code
                       AND con.destination_country = t.destination_country
         LEFT JOIN dwh.d_cargo c
                   ON c.cargo_code = t.cargo_code
ORDER BY
    t.shipment_date,
    t.wagon_number,
    t.container_number,
    t.document_number,
    t.volume,
    t.tariff,
    s.id IS NULL,
    t.load_dttm DESC
ON CONFLICT (
    shipment_date,
    wagon_number,
    container_number,
    document_number,
    volume,
    tariff
    )
    DO UPDATE
    SET
        shipment_category = EXCLUDED.shipment_category,
        cargo_class = EXCLUDED.cargo_class,
        estimated_arrival_date = EXCLUDED.estimated_arrival_date,
        arrival_date = EXCLUDED.arrival_date,
        release_date = EXCLUDED.release_date,
        transport_type = EXCLUDED.transport_type,

        cargo_id = EXCLUDED.cargo_id,
        sender_id = EXCLUDED.sender_id,
        wagon_id = EXCLUDED.wagon_id,
        consignee_id = EXCLUDED.consignee_id,

        wagon_km = EXCLUDED.wagon_km,
        volume = EXCLUDED.volume,
        tariff = EXCLUDED.tariff,

        load_dttm = EXCLUDED.load_dttm,
        source_file_name = EXCLUDED.source_file_name
WHERE dwh.f_cargo_shipments.load_dttm < EXCLUDED.load_dttm;


-- MERGE DATAMART
MERGE INTO datamart.universal_datamart AS ud
USING (
    SELECT *
    FROM (
        SELECT
            cs.shipment_date,
            cs.wagon_number,
            cs.container_number,
            cs.document_number,
            cs.shipment_category,
            cs.cargo_class,
            cs.estimated_arrival_date,
            cs.arrival_date,
            cs.release_date,
            cs.transport_type,
            cs.wagon_km,
            cs.volume,
            cs.tariff,
            cs.load_dttm,

            s.departure_station_sng_code,
            s.departure_railway,
            s.departure_country,
            s.departure_station_sng,
            s.departure_region,
            s.departure_station_rus,
            s.departure_station_rus_code,
            s.sender,
            s.sender_okpo,
            s.payer,
            
            c.cargo_code,
            c.cargo_name,

            w.wagon_type_group,
            w.wagon_type,
            w.wagon_owner,
            w.renter,
            w.operator,

            con.destination_country,
            con.destination_region,
            con.destination_railway,
            con.destination_station_rus,
            con.destination_station_rus_code,
            con.destination_station_sng,
            con.destination_station_sng_code,
            con.receiver,
            con.receiver_okpo,

            ROW_NUMBER() OVER (
                PARTITION BY cs.shipment_date, cs.wagon_number, cs.container_number,
                             cs.document_number, cs.volume, cs.tariff
                ORDER BY cs.load_dttm DESC
            ) AS rn
        FROM dwh.f_cargo_shipments cs 
            LEFT JOIN dwh.d_cargo c ON cs.cargo_id = c.id
            LEFT JOIN dwh.d_sender s ON cs.sender_id = s.id
            LEFT JOIN dwh.d_wagon w ON cs.wagon_id = w.id
            LEFT JOIN dwh.d_consignee con ON cs.consignee_id = con.id
    ) ranked
    WHERE rn = 1
) AS t
ON  ud.shipment_date = t.shipment_date
    AND ud.wagon_number = t.wagon_number
    AND ud.container_number = t.container_number
    AND ud.document_number = t.document_number
    AND ud.volume = t.volume
    AND ud.tariff = t.tariff
WHEN MATCHED AND ud.load_dttm < t.load_dttm THEN
    UPDATE SET
        shipment_date = t.shipment_date,
        wagon_number = t.wagon_number,
        container_number = t.container_number,
        document_number = t.document_number,
        shipment_category = t.shipment_category,
        cargo_class = t.cargo_class,
        estimated_arrival_date = t.estimated_arrival_date,
        arrival_date = t.arrival_date,
        release_date = t.release_date,
        transport_type = t.transport_type,
        wagon_km = t.wagon_km,
        volume = t.volume,
        tariff = t.tariff,
        load_dttm = t.load_dttm,

        departure_station_sng_code = t.departure_station_sng_code,
        departure_railway = t.departure_railway,
        departure_country = t.departure_country,
        departure_station_sng = t.departure_station_sng,
        departure_region = t.departure_region,
        departure_station_rus = t.departure_station_rus,
        departure_station_rus_code = t.departure_station_rus_code,
        sender = t.sender,
        sender_okpo = t.sender_okpo,
        payer = t.payer,

        wagon_type_group = t.wagon_type_group,
        wagon_type = t.wagon_type,
        wagon_owner = t.wagon_owner,
        renter = t.renter,
        operator = t.operator,

        destination_country = t.destination_country,
        destination_region = t.destination_region,
        destination_railway = t.destination_railway,
        destination_station_rus = t.destination_station_rus,
        destination_station_rus_code = t.destination_station_rus_code,
        destination_station_sng = t.destination_station_sng,
        destination_station_sng_code = t.destination_station_sng_code,
        receiver = t.receiver,
        receiver_okpo = t.receiver_okpo,

        cargo_code = t.cargo_code,
        cargo_name = t.cargo_name
WHEN NOT MATCHED THEN
INSERT (
    shipment_date, wagon_number, container_number, document_number,
    shipment_category, cargo_class, estimated_arrival_date, arrival_date,
    release_date, transport_type, wagon_km, volume, tariff, load_dttm,
    departure_station_sng_code, departure_railway, departure_country,
    departure_station_sng, departure_region, departure_station_rus,
    departure_station_rus_code, sender, sender_okpo, payer,
    wagon_type_group, wagon_type, wagon_owner, renter, operator,
    destination_country, destination_region, destination_railway,
    destination_station_rus, destination_station_rus_code,
    destination_station_sng, destination_station_sng_code,
    receiver, receiver_okpo, cargo_code, cargo_name
)
VALUES (
    t.shipment_date, t.wagon_number, t.container_number, t.document_number,
    t.shipment_category, t.cargo_class, t.estimated_arrival_date, t.arrival_date,
    t.release_date, t.transport_type, t.wagon_km, t.volume, t.tariff, t.load_dttm,
    t.departure_station_sng_code, t.departure_railway, t.departure_country,
    t.departure_station_sng, t.departure_region, t.departure_station_rus,
    t.departure_station_rus_code, t.sender, t.sender_okpo, t.payer,
    t.wagon_type_group, t.wagon_type, t.wagon_owner, t.renter, t.operator,
    t.destination_country, t.destination_region, t.destination_railway,
    t.destination_station_rus, t.destination_station_rus_code,
    t.destination_station_sng, t.destination_station_sng_code,
    t.receiver, t.receiver_okpo, t.cargo_code, t.cargo_name
);

--TRUNCATE TABLE staging.temp_table;
--TRUNCATE TABLE staging.temp_table_dublicates RESTART IDENTITY;
