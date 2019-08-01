import psycopg2

targ_tab = 'hotels'  # targ means 'target'
targ_sch = 'public'


def create_hotels(tab, schema):
    """
    :param tab: str
    :param schema: str
    :return:
    """
    pw = input('Pls input password for account "davis": ')
    conn = psycopg2.connect(
                                                database="hami",
                                                user="davis",
                                                password=pw,
                                                host="127.0.0.1",
                                                port="5432")

    cur = conn.cursor()
    print("--> Database opened successfully")

    table_nm = schema + '.' + tab

    cmd = f"""
                        CREATE    TABLE    {table_nm}
                        (
                            hotel_id                     text                 NOT NULL,
                            hotel_name              text                 NOT NULL,
                            酒店名称                    text                 NOT NULL,
                            城市                            text                 NOT NULL,
                            酒店位置                    text                 NOT NULL,
                            详细地址                    text                 NOT NULL,
                            客房总数                    bigint,
                            生效时间          timestamp with time zone          DEFAULT  current_timestamp,
                            修改用户                    text                    DEFAULT  current_user,  
                            PRIMARY KEY (hotel_id),
                            CONSTRAINT uni_hotelnm UNIQUE (hotel_name),
                            CONSTRAINT uni_酒店名称 UNIQUE (酒店名称)
                        )
                        
                        WITH
                        (
                            OIDS = FALSE
                        );
                        
                        ALTER TABLE {table_nm}
                            OWNER to davis;
                        """

    cur.execute(cmd)
    conn.commit()
    conn.close()

    print("--> Main table created successfully\n")


def create_hotels_audit(tab, schema):
    """

    :param tab: target table
    :param schema: target schema
    :return:
    """
    pw = input('Pls input password for account "davis": ')
    conn = psycopg2.connect(
        database="hami",
        user="davis",
        password=pw,
        host="127.0.0.1",
        port="5432")

    cur = conn.cursor()
    print("--> Database opened successfully")

    audit_tab = 'history' + '.' + tab + '_audit'
    target_tab = schema + '.' + tab

    cmd = f"""
    -- Step 1: create an audit table
    CREATE TABLE {audit_tab}(
        row_id            bigserial                                       NOT NULL,
        operation         char(1)                                        NOT NULL,
        op_time           timestamp with time zone      NOT NULL, 
        op_usr             text                                              NOT NULL,
        PRIMARY KEY (row_id),
        like {target_tab}
        INCLUDING DEFAULTS        
    );
        
    -- Step 2: create a trigger function
    CREATE OR REPLACE FUNCTION {tab}_audit() RETURNS TRIGGER AS ${tab}_audit$
        BEGIN
            --
            -- Create a row in history.{tab}_audit to reflect the operation performed on {target_tab},
            -- making use of the special variable TG_OP to work out the operation.
            --
            IF (TG_OP = 'DELETE') THEN
                INSERT INTO {audit_tab} VALUES (DEFAULT, 'D', current_timestamp, current_user, OLD.*);
            ELSIF (TG_OP = 'UPDATE') THEN
                INSERT INTO {audit_tab} VALUES (DEFAULT, 'U', current_timestamp, current_user, OLD.*);
            ELSIF (TG_OP = 'INSERT') THEN
                INSERT INTO {audit_tab} VALUES (DEFAULT, 'I', current_timestamp, current_user, NEW.*);
            END IF;
            RETURN NULL; -- result is ignored since this is an AFTER trigger
        END;
    ${tab}_audit$ LANGUAGE plpgsql;
    
    -- Step 3: create the actual trigger...
    CREATE TRIGGER {tab}_audit
    AFTER INSERT OR UPDATE OR DELETE ON {target_tab}
    FOR EACH ROW EXECUTE FUNCTION {tab}_audit();
        
    """
    cur.execute(cmd)
    conn.commit()
    conn.close()

    print("--> Audit table created successfully\n")


bline = "======================================="

create_hotels(tab=targ_tab, schema=targ_sch)

print(bline)

create_hotels_audit(tab=targ_tab, schema=targ_sch)

print('--- END OF PROGRAM ---')
