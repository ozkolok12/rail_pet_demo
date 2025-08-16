-- CREATING nesessary tables in  schemas
-- cargo senders data
CREATE TABLE IF NOT EXISTS dwh.d_sender (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    departure_station_sng_code BIGINT,
    departure_railway VARCHAR(3),
    departure_country VARCHAR(60),
    departure_station_sng VARCHAR(60),
    departure_region VARCHAR(60),
    departure_station_rus VARCHAR(60),
    departure_station_rus_code BIGINT,
    sender TEXT,
    sender_okpo VARCHAR(60),
    payer TEXT,
    load_dttm TIMESTAMP,
    source_file_name TEXT
);

ALTER TABLE dwh.d_sender
    ADD CONSTRAINT d_sender_uq
        UNIQUE (departure_country,
                sender,
                sender_okpo,
                payer,
                departure_station_rus,
                departure_station_sng
               );

COMMENT ON TABLE dwh.d_sender IS 'Dimension table for cargo senders with their details';
COMMENT ON COLUMN dwh.d_sender.departure_station_sng_code IS 'Код станции отправления СНГ';
COMMENT ON COLUMN dwh.d_sender.departure_railway IS 'Дорога отправления';
COMMENT ON COLUMN dwh.d_sender.departure_country IS 'Государство отправления';
COMMENT ON COLUMN dwh.d_sender.departure_station_sng IS 'Станция отправления СНГ';
COMMENT ON COLUMN dwh.d_sender.departure_region IS 'Область отправления';
COMMENT ON COLUMN dwh.d_sender.departure_station_rus IS 'Станция отправления РФ';
COMMENT ON COLUMN dwh.d_sender.departure_station_rus_code IS 'Код станции отправления РФ';
COMMENT ON COLUMN dwh.d_sender.sender IS 'Грузоотправитель';
COMMENT ON COLUMN dwh.d_sender.sender_okpo IS 'Грузоотправитель (ОКПО)';
COMMENT ON COLUMN dwh.d_sender.payer IS 'Плательщик';
COMMENT ON COLUMN dwh.d_sender.load_dttm IS 'Время загрузки данных';

-- wagon details
CREATE TABLE IF NOT EXISTS dwh.d_wagon (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    wagon_type_group VARCHAR(60),
    wagon_type VARCHAR(100),
    wagon_owner TEXT,
    renter TEXT,
    operator TEXT,
    load_dttm TIMESTAMP,
    source_file_name TEXT
);

ALTER TABLE dwh.d_wagon
    ADD CONSTRAINT d_wagon_uq
        UNIQUE (
                wagon_type,
                wagon_owner,
                renter,
                operator
            );

COMMENT ON TABLE dwh.d_wagon IS 'Wagon types details, which are used for cargo shipment';
COMMENT ON COLUMN dwh.d_wagon.wagon_type_group IS 'Род вагона';
COMMENT ON COLUMN dwh.d_wagon.wagon_type IS 'Тип вагона';
COMMENT ON COLUMN dwh.d_wagon.wagon_owner IS 'Собственник';
COMMENT ON COLUMN dwh.d_wagon.renter IS 'Арендатор';
COMMENT ON COLUMN dwh.d_wagon.operator IS 'Оператор';
COMMENT ON COLUMN dwh.d_wagon.load_dttm IS 'Время загрузки данных';

-- consignee details
CREATE TABLE IF NOT EXISTS dwh.d_consignee (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    destination_country VARCHAR(60),
    destination_region VARCHAR(60),
    destination_railway VARCHAR(3),
    destination_station_rus VARCHAR(60),
    destination_station_rus_code BIGINT,
    destination_station_sng VARCHAR(60),
    destination_station_sng_code BIGINT,
    receiver TEXT,
    receiver_okpo TEXT,
    load_dttm TIMESTAMP,
    source_file_name TEXT
);

ALTER TABLE dwh.d_consignee
    ADD CONSTRAINT d_consignee_uq
        UNIQUE (
                destination_country,
                receiver,
                receiver_okpo,
                destination_station_rus,
                destination_station_sng
            );

COMMENT ON TABLE dwh.d_consignee IS 'Consignee details and location';
COMMENT ON COLUMN dwh.d_consignee.destination_country IS 'Государство назначения';
COMMENT ON COLUMN dwh.d_consignee.destination_region IS 'Область назначения';
COMMENT ON COLUMN dwh.d_consignee.destination_railway IS 'Дорога назначения';
COMMENT ON COLUMN dwh.d_consignee.destination_station_rus IS 'Станция назначения РФ';
COMMENT ON COLUMN dwh.d_consignee.destination_station_rus_code IS 'Код станции назначения РФ';
COMMENT ON COLUMN dwh.d_consignee.destination_station_sng IS 'Станция назначения СНГ';
COMMENT ON COLUMN dwh.d_consignee.destination_station_sng_code IS 'Код станции назначения СНГ';
COMMENT ON COLUMN dwh.d_consignee.receiver IS 'Грузополучатель';
COMMENT ON COLUMN dwh.d_consignee.receiver_okpo IS 'Грузополучатель (ОКПО)';
COMMENT ON COLUMN dwh.d_consignee.load_dttm IS 'Время загрузки данных';

-- cargo types details
CREATE TABLE IF NOT EXISTS dwh.d_cargo (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    cargo_code BIGINT,
    cargo_name TEXT,
    load_dttm TIMESTAMP,
    source_file_name TEXT
);

COMMENT ON COLUMN dwh.d_cargo.cargo_code IS 'Код груза';
COMMENT ON COLUMN dwh.d_cargo.cargo_name IS 'Груз';
COMMENT ON COLUMN dwh.d_cargo.load_dttm IS 'Время загрузки данных';

-- main fact table in star scheme, with shipments by dates
CREATE TABLE IF NOT EXISTS dwh.f_cargo_shipments (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    shipment_date DATE,
    wagon_number BIGINT,
    container_number TEXT,
    document_number TEXT,
    shipment_category TEXT,
    cargo_class SMALLINT,
    estimated_arrival_date DATE,
    arrival_date DATE,
    release_date DATE,
    transport_type VARCHAR(15),
    
    cargo_id SMALLINT,
    sender_id SMALLINT,
    wagon_id SMALLINT,
    consignee_id SMALLINT,

    wagon_km BIGINT,
    volume NUMERIC(10,2),
    tariff BIGINT,
    load_dttm TIMESTAMP,
    source_file_name TEXT,
    
    CONSTRAINT cargo_fk FOREIGN KEY (cargo_id) REFERENCES dwh.d_cargo(id),
    CONSTRAINT sender_fk FOREIGN KEY (sender_id) REFERENCES dwh.d_sender(id),
    CONSTRAINT wagon_fk FOREIGN KEY (wagon_id) REFERENCES dwh.d_wagon(id),
    CONSTRAINT consignee_fk FOREIGN KEY (consignee_id) REFERENCES dwh.d_consignee(id)
);

ALTER TABLE dwh.f_cargo_shipments
    ADD CONSTRAINT d_f_cargo_shipments_uq
        UNIQUE (
                shipment_date,
                wagon_number,
                container_number,
                document_number,
                volume,
                tariff
            );

COMMENT ON COLUMN dwh.f_cargo_shipments.shipment_date IS 'Дата отправления';
COMMENT ON COLUMN dwh.f_cargo_shipments.wagon_number IS 'Номер вагона';
COMMENT ON COLUMN dwh.f_cargo_shipments.container_number IS 'Номер контейнера';
COMMENT ON COLUMN dwh.f_cargo_shipments.document_number IS 'Номер документа';
COMMENT ON COLUMN dwh.f_cargo_shipments.shipment_category IS 'Категория отправки';
COMMENT ON COLUMN dwh.f_cargo_shipments.cargo_class IS 'Класс груза';
COMMENT ON COLUMN dwh.f_cargo_shipments.estimated_arrival_date IS 'Прогнозная дата прибытия';
COMMENT ON COLUMN dwh.f_cargo_shipments.arrival_date IS 'Дата прибытия';
COMMENT ON COLUMN dwh.f_cargo_shipments.release_date IS 'Дата раскредитования';
COMMENT ON COLUMN dwh.f_cargo_shipments.transport_type IS 'Вид перевозки';
COMMENT ON COLUMN dwh.f_cargo_shipments.wagon_km IS 'Вагоно-км';
COMMENT ON COLUMN dwh.f_cargo_shipments.volume IS 'Объем';
COMMENT ON COLUMN dwh.f_cargo_shipments.tariff IS 'Тариф';
COMMENT ON COLUMN dwh.f_cargo_shipments.load_dttm IS 'Время загрузки данных';

COMMENT ON COLUMN dwh.f_cargo_shipments.cargo_id IS 'Foreign key to d_cargo table';
COMMENT ON COLUMN dwh.f_cargo_shipments.wagon_id IS 'Foreign key to d_wagon table';
COMMENT ON COLUMN dwh.f_cargo_shipments.sender_id IS 'Foreign key to d_sender table';
COMMENT ON COLUMN dwh.f_cargo_shipments.consignee_id IS 'Foreign key to d_consignee table';

-- temp  table for staging
CREATE TABLE IF NOT EXISTS staging.temp_table (
    stg_id BIGINT,
    shipment_date DATE,                               -- Дата отправления
    wagon_number BIGINT,                              -- Номер вагона
    container_number TEXT,                            -- Номер контейнера
    document_number TEXT,                             -- Номер документа
    shipment_category VARCHAR(60),                    -- Категория отправки
    cargo_class SMALLINT,                             -- Класс груза
    estimated_arrival_date DATE,                      -- Прогнозная дата прибытия
    tonnage_type VARCHAR(60),                         -- Тоннажность
    arrival_date DATE,                                -- Дата прибытия
    release_date DATE,                                -- Дата раскредитования
    transport_type VARCHAR(15),                       -- Вид перевозки
    cargo_code BIGINT,                                -- Код груза
    cargo_name TEXT,                                  -- Груз
    departure_country VARCHAR(60),                    -- Государство отправления
    departure_station_sng VARCHAR(60),                -- Станция отправления СНГ
    departure_station_sng_code BIGINT,                -- Код станции отправления СНГ
    departure_region TEXT,                     -- Область отправления
    departure_railway VARCHAR(3),                     -- Дорога отправления
    departure_station_rus VARCHAR(60),                -- Станция отправления РФ
    departure_station_rus_code BIGINT,                -- Код станции отправления РФ
    sender TEXT,                                      -- Грузоотправитель
    sender_okpo VARCHAR(60),                          -- Грузоотправитель (ОКПО)
    destination_country VARCHAR(60),                  -- Государство назначения
    destination_region TEXT,                   -- Область назначения
    destination_railway VARCHAR(3),                   -- Дорога назначения
    destination_station_rus VARCHAR(60),              -- Станция назначения РФ
    destination_station_rus_code BIGINT,              -- Код станции назначения РФ
    destination_station_sng VARCHAR(60),              -- Станция назначения СНГ
    destination_station_sng_code BIGINT,              -- Код станции назначения СНГ
    receiver TEXT,                                    -- Грузополучатель
    receiver_okpo TEXT,                               -- Грузополучатель (ОКПО)
    wagon_type_group VARCHAR(60),                     -- Род вагона
    wagon_type VARCHAR(100),                          -- Тип вагона
    payer TEXT,                                       -- Плательщик
    wagon_owner TEXT,                                 -- Собственник
    renter TEXT,                                      -- Арендатор
    operator TEXT,                                    -- Оператор
    wagon_km BIGINT,                                  -- Вагоно-км
    volume NUMERIC(10,2),                             -- Объем
    tariff BIGINT,
    load_dttm TIMESTAMP,                                -- Тариф
    source_file_name TEXT
);


COMMENT ON COLUMN staging.temp_table.shipment_date IS 'Дата отправления';
COMMENT ON COLUMN staging.temp_table.wagon_number IS 'Номер вагона';
COMMENT ON COLUMN staging.temp_table.container_number IS 'Номер контейнера';
COMMENT ON COLUMN staging.temp_table.document_number IS 'Номер документа';
COMMENT ON COLUMN staging.temp_table.shipment_category IS 'Категория отправки';
COMMENT ON COLUMN staging.temp_table.cargo_class IS 'Класс груза';
COMMENT ON COLUMN staging.temp_table.estimated_arrival_date IS 'Прогнозная дата прибытия';
COMMENT ON COLUMN staging.temp_table.tonnage_type IS 'Тоннажность';
COMMENT ON COLUMN staging.temp_table.arrival_date IS 'Дата прибытия';
COMMENT ON COLUMN staging.temp_table.release_date IS 'Дата раскредитования';
COMMENT ON COLUMN staging.temp_table.transport_type IS 'Вид перевозки';
COMMENT ON COLUMN staging.temp_table.cargo_code IS 'Код груза';
COMMENT ON COLUMN staging.temp_table.cargo_name IS 'Груз';
COMMENT ON COLUMN staging.temp_table.departure_country IS 'Государство отправления';
COMMENT ON COLUMN staging.temp_table.departure_station_sng IS 'Станция отправления СНГ';
COMMENT ON COLUMN staging.temp_table.departure_station_sng_code IS 'Код станции отправления СНГ';
COMMENT ON COLUMN staging.temp_table.departure_region IS 'Область отправления';
COMMENT ON COLUMN staging.temp_table.departure_railway IS 'Дорога отправления';
COMMENT ON COLUMN staging.temp_table.departure_station_rus IS 'Станция отправления РФ';
COMMENT ON COLUMN staging.temp_table.departure_station_rus_code IS 'Код станции отправления РФ';
COMMENT ON COLUMN staging.temp_table.sender IS 'Грузоотправитель';
COMMENT ON COLUMN staging.temp_table.sender_okpo IS 'Грузоотправитель (ОКПО)';
COMMENT ON COLUMN staging.temp_table.destination_country IS 'Государство назначения';
COMMENT ON COLUMN staging.temp_table.destination_region IS 'Область назначения';
COMMENT ON COLUMN staging.temp_table.destination_railway IS 'Дорога назначения';
COMMENT ON COLUMN staging.temp_table.destination_station_rus IS 'Станция назначения РФ';
COMMENT ON COLUMN staging.temp_table.destination_station_rus_code IS 'Код станции назначения РФ';
COMMENT ON COLUMN staging.temp_table.destination_station_sng IS 'Станция назначения СНГ';
COMMENT ON COLUMN staging.temp_table.destination_station_sng_code IS 'Код станции назначения СНГ';
COMMENT ON COLUMN staging.temp_table.receiver IS 'Грузополучатель';
COMMENT ON COLUMN staging.temp_table.receiver_okpo IS 'Грузополучатель (ОКПО)';
COMMENT ON COLUMN staging.temp_table.wagon_type_group IS 'Род вагона';
COMMENT ON COLUMN staging.temp_table.wagon_type IS 'Тип вагона';
COMMENT ON COLUMN staging.temp_table.payer IS 'Плательщик';
COMMENT ON COLUMN staging.temp_table.wagon_owner IS 'Собственник';
COMMENT ON COLUMN staging.temp_table.renter IS 'Арендатор';
COMMENT ON COLUMN staging.temp_table.operator IS 'Оператор';
COMMENT ON COLUMN staging.temp_table.wagon_km IS 'Вагоно-км';
COMMENT ON COLUMN staging.temp_table.volume IS 'Объем';
COMMENT ON COLUMN staging.temp_table.tariff IS 'Тариф';



-- universal datamart with all columns
CREATE TABLE IF NOT EXISTS datamart.universal_datamart (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    shipment_date DATE,
    wagon_number BIGINT,
    container_number TEXT,
    document_number TEXT,
    shipment_category TEXT,
    cargo_class SMALLINT,
    estimated_arrival_date DATE,
    arrival_date DATE,
    release_date DATE,
    transport_type VARCHAR(15),
    wagon_km BIGINT,
    volume NUMERIC(10,2),
    tariff BIGINT,
    load_dttm TIMESTAMP,

    -- sender block
    departure_station_sng_code BIGINT,
    departure_railway VARCHAR(3),
    departure_country VARCHAR(60),
    departure_station_sng VARCHAR(60),
    departure_region VARCHAR(60),
    departure_station_rus VARCHAR(60),
    departure_station_rus_code BIGINT,
    sender TEXT,
    sender_okpo VARCHAR(60),
    payer TEXT,

    -- wagon block
    wagon_type_group VARCHAR(60),
    wagon_type VARCHAR(100),
    wagon_owner TEXT,
    renter TEXT,
    operator TEXT,

    -- consignee block
    destination_country VARCHAR(60),
    destination_region VARCHAR(60),
    destination_railway VARCHAR(3),
    destination_station_rus VARCHAR(60),
    destination_station_rus_code BIGINT,
    destination_station_sng VARCHAR(60),
    destination_station_sng_code BIGINT,
    receiver TEXT,
    receiver_okpo TEXT,

    -- cargo block
    cargo_code BIGINT,
    cargo_name TEXT
);

-- creating staging table for wagon
CREATE TABLE IF NOT EXISTS staging.temp_table_wagon (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    wagon_type_group VARCHAR(60),
    wagon_type VARCHAR(100),
    wagon_owner TEXT,
    renter TEXT,
    operator TEXT,
    load_dttm TIMESTAMP,
    source_file_name TEXT
);

-- creating staging table for senders
CREATE TABLE IF NOT EXISTS staging.temp_table_sender (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    departure_station_sng_code BIGINT,
    departure_railway VARCHAR(3),
    departure_country VARCHAR(60),
    departure_station_sng VARCHAR(60),
    departure_region VARCHAR(60),
    departure_station_rus VARCHAR(60),
    departure_station_rus_code BIGINT,
    sender TEXT,
    sender_okpo VARCHAR(60),
    payer TEXT,
    load_dttm TIMESTAMP,
    source_file_name TEXT
);

-- creating staging table for consignee
CREATE TABLE IF NOT EXISTS staging.temp_table_consignee (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    destination_country VARCHAR(60),
    destination_region VARCHAR(60),
    destination_railway VARCHAR(3),
    destination_station_rus VARCHAR(60),
    destination_station_rus_code BIGINT,
    destination_station_sng VARCHAR(60),
    destination_station_sng_code BIGINT,
    receiver TEXT,
    receiver_okpo TEXT,
    load_dttm TIMESTAMP,
    source_file_name TEXT
);

-- creating staging table for fact
CREATE TABLE IF NOT EXISTS staging.temp_table_dublicates (
      stg_id BIGINT,
      shipment_date DATE,                               -- Дата отправления
      wagon_number BIGINT,                              -- Номер вагона
      container_number TEXT,                            -- Номер контейнера
      document_number TEXT,                             -- Номер документа
      shipment_category VARCHAR(60),                    -- Категория отправки
      cargo_class SMALLINT,                             -- Класс груза
      estimated_arrival_date DATE,                      -- Прогнозная дата прибытия
      tonnage_type VARCHAR(60),                         -- Тоннажность
      arrival_date DATE,                                -- Дата прибытия
      release_date DATE,                                -- Дата раскредитования
      transport_type VARCHAR(15),                       -- Вид перевозки
      cargo_code BIGINT,                                -- Код груза
      cargo_name TEXT,                                  -- Груз
      departure_country VARCHAR(60),                    -- Государство отправления
      departure_station_sng VARCHAR(60),                -- Станция отправления СНГ
      departure_station_sng_code BIGINT,                -- Код станции отправления СНГ
      departure_region TEXT,                     -- Область отправления
      departure_railway VARCHAR(3),                     -- Дорога отправления
      departure_station_rus VARCHAR(60),                -- Станция отправления РФ
      departure_station_rus_code BIGINT,                -- Код станции отправления РФ
      sender TEXT,                                      -- Грузоотправитель
      sender_okpo VARCHAR(60),                          -- Грузоотправитель (ОКПО)
      destination_country VARCHAR(60),                  -- Государство назначения
      destination_region TEXT,                   -- Область назначения
      destination_railway VARCHAR(3),                   -- Дорога назначения
      destination_station_rus VARCHAR(60),              -- Станция назначения РФ
      destination_station_rus_code BIGINT,              -- Код станции назначения РФ
      destination_station_sng VARCHAR(60),              -- Станция назначения СНГ
      destination_station_sng_code BIGINT,              -- Код станции назначения СНГ
      receiver TEXT,                                    -- Грузополучатель
      receiver_okpo TEXT,                               -- Грузополучатель (ОКПО)
      wagon_type_group VARCHAR(60),                     -- Род вагона
      wagon_type VARCHAR(100),                          -- Тип вагона
      payer TEXT,                                       -- Плательщик
      wagon_owner TEXT,                                 -- Собственник
      renter TEXT,                                      -- Арендатор
      operator TEXT,                                    -- Оператор
      wagon_km BIGINT,                                  -- Вагоно-км
      volume NUMERIC(10,2),                             -- Объем
      tariff BIGINT,
      load_dttm TIMESTAMP,                                -- Тариф
      source_file_name TEXT
);

-- creating staging table for cargo
CREATE TABLE IF NOT EXISTS staging.temp_table_cargo (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    cargo_code BIGINT,
    cargo_name TEXT,
    load_dttm TIMESTAMP,
    source_file_name TEXT
);

-- minimal logs table
CREATE TABLE IF NOT EXISTS staging.load_log (
    log_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_table TEXT,
    target_table TEXT,
    file_name TEXT,
    rows_inserted INTEGER,
    status TEXT CHECK (status IN ('success', 'failed')),
    error_message TEXT
);

-- Create table for message_id comparison
CREATE TABLE IF NOT EXISTS etl.processed_files (
    message_id VARCHAR PRIMARY KEY,
    filename VARCHAR NOT NULL,
    file_size INT NOT NULL,
    processed_at TIMESTAMP DEFAULT now()
);
