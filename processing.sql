DO
$do$
DECLARE
-- THE FOLLOWING VARIABLES CAN BE CHANGED BY THE USER FOR VARIOUS SETTINGS --
-----------------------------------------------------------------------------
_fresh int := 1; -- Is this a fresh run of the processing or a re-run using previously processed data. NOTE: Setting this to 0 is currently not fully tested.
_clean int := 1; -- Should tables created for the processing be cleaned up afterwards. This removes all intermediate data, leaving only the start data and final result.
_run int := 1; -- If set to 0 only the initial setup will run. Very useful when debugging queries or wanting to fine tune performance.
_schema text := 'openrivers'; -- The schema name to be used. The executing user would already be connected to a relevant database before running the processing.
_proc_table text := 'rivers'; -- Name to use for the processing table. Will be removed on completion if "clean" is set to 1.
_nodes_table text := 'hydronode'; -- Table name of the OS OpenRivers Hydronode table. hnsub for testing.
_links_table text := 'watercourselink'; -- Table name of the OS OpenRivers Watercourselink table. wcsub for testing.
_out_table text := 'output'; -- Name of the final output table. testout for testing.
_buffer_radius int := 1000; -- The buffer radius in m to use for the final collection. This can be fine tuned, thought the default value of 1000 appears sufficient in testing with no confirmed outliers or false positives found.
-----------------------------------------------------------------------------
-- DO NOT CHANGE ANYTHING BELOW THIS LINE UNLESS YOU KNOW THE EFFECT --
_proc text := concat(_schema||'.'||_proc_table);
_nodes text := concat(_schema||'.'||_nodes_table);
_links text := concat(_schema||'.'||_links_table);
_out text := concat(_schema||'.'||_out_table);
_counter bigint := 0;
_check bigint := 1;
_subcounter bigint := 0;
_subcheck bigint := 1;
_inserts_made bigint := 1;
_subinserts_made bigint := 1;
_combined bigint := 1;
BEGIN
    
    RAISE NOTICE 'Starting the processing...';
    
    
    if _fresh = 1 THEN -- As mentioned in the variables section, changing "fresh" to 0 will likely result in unplanned behaviour.
        
        EXECUTE ('DROP TABLE IF EXISTS '||_proc); -- Remove the old processing table if it exists.
        -- Create the processing table as a fresh table including adding the relevant geometry entries.
        EXECUTE ('CREATE TABLE '||_proc||' (gid serial, "name1" varchar(250), "name2" varchar(250), "link" varchar(38), "identifier" varchar(38), "startnode" varchar(38), "endnode" varchar(38), "length" int)');
        PERFORM AddGeometryColumn(_schema,_proc_table,'geom','0','MULTILINESTRING',4);
        PERFORM UpdateGeometrySRID(_schema,_proc_table,'geom',27700);
    
        -- Update the nodes and links tables to conform to the process geometry.
        PERFORM UpdateGeometrySRID(_schema,_links_table,'geom',27700);
        PERFORM UpdateGeometrySRID(_schema,_nodes_table,'geom',27700);
    
        -- We drop, then recreate all relevant indexes for the processing. This ensures indexes are up-to-date should any errors have interrupted previous processing.
        EXECUTE ('DROP INDEX IF EXISTS '||_schema||'.wc_startnode');
        EXECUTE ('CREATE INDEX wc_startnode on '||_links||' (startnode)');
        EXECUTE ('DROP INDEX IF EXISTS '||_schema||'.wc_endnode');
        EXECUTE ('CREATE INDEX wc_endnode on '||_links||' (endnode)');
        EXECUTE ('DROP INDEX IF EXISTS '||_schema||'.wc_identifier');
        EXECUTE ('CREATE INDEX wc_identifier on '||_links||' (identifier)');
        EXECUTE ('DROP INDEX IF EXISTS '||_schema||'.hn_identifier');
        EXECUTE ('CREATE INDEX hn_identifier on '||_nodes||' (identifier)');
        EXECUTE ('DROP INDEX IF EXISTS '||_schema||'.ri_endnode');
        EXECUTE ('CREATE INDEX ri_endnode on '||_proc||' (endnode)');
        EXECUTE ('DROP INDEX IF EXISTS '||_schema||'.ri_link');
        EXECUTE ('CREATE INDEX ri_link on '||_proc||' (link)');
        EXECUTE ('DROP INDEX IF EXISTS '||_schema||'.ri_identifier');
        EXECUTE ('CREATE INDEX ri_identifier on '||_proc||' (identifier)');
    
        RAISE NOTICE 'Indexes prepared.';
        
        -- The links table will contain closed loop links denoting ponds and small lakes. These are disruptive to the processing and cause unexpected and inconsistent errors. We simply remove them as testing has shown them to be irrelevant to the overall result.
        EXECUTE ('delete from '||_links||' where startnode = endnode');
        
        -- The nodes table requires a few changes to ensure we have the correct link counts for each node.
        EXECUTE ('ALTER TABLE '||_nodes||' DROP COLUMN IF EXISTS starts');
        EXECUTE ('ALTER TABLE '||_nodes||' DROP COLUMN IF EXISTS ends');
        EXECUTE ('ALTER TABLE '||_nodes||' DROP COLUMN IF EXISTS nodecount');
        
        EXECUTE ('ALTER TABLE '||_nodes||' ADD COLUMN starts bigint');
        EXECUTE ('ALTER TABLE '||_nodes||' ADD COLUMN ends bigint');
        EXECUTE ('ALTER TABLE '||_nodes||' ADD COLUMN nodecount bigint');
    
        EXECUTE ('UPDATE '||_nodes||' set nodecount = 0, starts = 0, ends = 0');
        EXECUTE ('UPDATE '||_nodes||' set starts = subquery.nodes FROM (select h.identifier, count(*) as nodes from '||_nodes||' h, '||_links||' w where h.identifier = w.startnode group by h.identifier) as subquery where '||_nodes||'.identifier = subquery.identifier');
        EXECUTE ('UPDATE '||_nodes||' set ends = subquery.nodes FROM (select h.identifier, count(*) as nodes from '||_nodes||' h, '||_links||' w where h.identifier = w.endnode group by h.identifier) as subquery where '||_nodes||'.identifier = subquery.identifier');
        EXECUTE ('UPDATE '||_nodes||' set nodecount = starts+ends');
    END IF;
    
    -- We clear out any temporary tables that have been created. Unlike the main processing table we recreate these on every run as the data is dynamic and considered disposable.
    perform n.nspname ,c.relname FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace where n.nspname like 'pg_temp_%' AND pg_catalog.pg_table_is_visible(c.oid) AND Upper(relname) = Upper('names');
    IF FOUND THEN
        Drop table pg_temp.names;
    END IF;
    create temporary table pg_temp.names (identifier varchar(38), name1 varchar(250), name2 varchar(250));
    EXECUTE ('insert into pg_temp.names (identifier, name1, name2) select identifier, name1, name2 from '||_proc||' where name1 is not null and name1 != '''' ');
    DROP INDEX IF EXISTS pg_temp.n_name1;
    DROP INDEX IF EXISTS pg_temp.n_name2;
    DROP INDEX IF EXISTS pg_temp.n_identifier;
    CREATE INDEX n_name1 on pg_temp.names (name1);
    CREATE INDEX n_name2 on pg_temp.names (name2);
    CREATE INDEX n_identifier on pg_temp.names (identifier);
    
    perform n.nspname ,c.relname FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace where n.nspname like 'pg_temp_%' AND pg_catalog.pg_table_is_visible(c.oid) AND Upper(relname) = Upper('lengths');
    IF FOUND THEN
        Drop table pg_temp.lengths;
    END IF;
    EXECUTE ('create temporary table pg_temp.lengths as select identifier, endnode, sum(length) as total from '||_proc||' group by identifier, endnode');
    DROP INDEX IF EXISTS pg_temp.l_identifier;
    CREATE INDEX l_identifier on pg_temp.lengths (identifier);
    DROP INDEX IF EXISTS pg_temp.l_endnode;
    CREATE INDEX l_endnode on pg_temp.lengths (endnode);
    
    perform n.nspname ,c.relname FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace where n.nspname like 'pg_temp_%' AND pg_catalog.pg_table_is_visible(c.oid) AND Upper(relname) = Upper('valid');
    IF FOUND THEN
        drop table pg_temp.valid;
    END IF;
    create temporary table pg_temp.valid (identifier varchar(38));
    DROP INDEX IF EXISTS pg_temp.v_identifier;
    CREATE INDEX v_identifier on pg_temp.valid (identifier);
    
    perform n.nspname ,c.relname FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace where n.nspname like 'pg_temp_%' AND pg_catalog.pg_table_is_visible(c.oid) AND Upper(relname) = Upper('filter');
    IF FOUND THEN
        Drop table pg_temp.filter;
    END IF;
    EXECUTE ('create temporary table pg_temp.filter (identifier varchar(38))');
    DROP INDEX IF EXISTS pg_temp.filter_identifier;
    CREATE INDEX filter_identifier on pg_temp.filter (identifier);
    
    -- This trigger function will update the temporary tables with relevant information, ensuring the lookups for complex queries are up-to-date.
    CREATE OR REPLACE FUNCTION rivers_insert()
        RETURNS trigger AS  
    $$
    DECLARE
        _tnodes regclass := TG_ARGV[0];
    BEGIN
        perform 1 from pg_temp.names where identifier = NEW.identifier;
        IF FOUND THEN
            UPDATE pg_temp.names set name1 = NEW.name1 where identifier = NEW.identifier and name1 IS DISTINCT FROM NEW.name1 and (name1 = '' or name1 IS NULL) and (NEW.name1 != '' and NEW.name1 is not null);
            UPDATE pg_temp.names set name2 = NEW.name2 where identifier = NEW.identifier and name2 IS DISTINCT FROM NEW.name2 and (name2 = '' or name2 IS NULL) and (NEW.name2 != '' and NEW.name2 is not null);
        ELSE
            INSERT INTO pg_temp.names(identifier,name1,name2) VALUES(NEW.identifier,NEW.name1,NEW.name2);
        END IF;
        perform 1 from pg_temp.lengths where identifier = NEW.identifier;
        IF FOUND THEN
            UPDATE pg_temp.lengths set total = total+NEW.length, endnode=NEW.endnode where identifier = NEW.identifier;
        ELSE
            INSERT into pg_temp.lengths (identifier, endnode, total) values (NEW.identifier, NEW.endnode, NEW.length);
        END IF;
        RETURN NEW;
    END;
    $$
    LANGUAGE 'plpgsql';
    
    -- We drop, then re-add the trigger ensuring consistency.
    EXECUTE ('DROP TRIGGER IF EXISTS t_names on '||_proc);
    EXECUTE ('CREATE TRIGGER t_names after insert on '||_proc||' for EACH ROW EXECUTE PROCEDURE rivers_insert('''||_nodes||''')');
    
    _check := 1; -- Check is used as a generic variable to ensure looping starts, continues and stops appropriately.

    RAISE NOTICE 'Prep done.';
    
    IF _run = 1 THEN
    
        EXECUTE ('insert into '||_proc||' (name1, name2, link, identifier, startnode, endnode, length, geom) select w.name1,w.name2,w.identifier,w.identifier,w.startnode,w.endnode,w.length,w.geom from '||_links||' w, '||_nodes||' h where h.starts >= 1 and h.ends = 0 and w.startnode = h.identifier and not exists (select identifier from '||_proc||' ri where ri.identifier = w.identifier)'); -- The first batch of data is inserted. Effectively, any links connected to nodes that exclusively START links but do NOT END any.
        
        -- From this point onwards the processing will use "live" streams only. A live stream is represented by any node that has all END connections attached, but not all START links are present. By default this results in a downstream motion of the processing, halting further processing until all merging streams are attached before continuing on a node.
        
        EXECUTE ('select count(*) from '||_proc) into _inserts_made;
        RAISE NOTICE 'Initial Run - % records present.',_inserts_made;
        
        _subinserts_made := 1;
        
        -- The following WHILE loop will loop until all links are accounted for in the processing. This may take around 24 hours or more depending on the processing capabilities of the host machine.
        WHILE _inserts_made >= 0 LOOP
            -- The subinserts loop connects 1:1 connections to the existing links. In other words if a node ends exactly one link and starts exactly one link the node is attached as is.
            -- This loop repeats until no 1:1 connections on "live" streams remain.
            WHILE _subinserts_made > 0 LOOP
                EXECUTE ('select count(*) from '||_proc) into _subcheck;
                
                -- The first statement will attach 1:1 links using the correct name if a corresponding upstream name exists in the temporary tables.
                EXECUTE ('insert into '||_proc||' (name1, name2, link, identifier, startnode, endnode, length, geom) select distinct w.name1,w.name2,w.identifier,r.identifier,w.startnode,w.endnode,w.length,w.geom from '||_links||' w, '||_proc||' r, '||_nodes||' h where not exists (select 1 from '||_proc||' where link = w.identifier) and w.startnode = h.identifier and h.starts = 1 and h.ends = 1 and r.endnode = w.startnode and exists (select n.name1 from pg_temp.names n where n.identifier = r.identifier and ((n.name1 IS NOT DISTINCT FROM w.name1) or (n.name1 IS DISTINCT FROM w.name1 and ((n.name1 = '''' or n.name1 IS NULL) or (w.name1 = '''' or w.name1 IS NULL)))))');
                
                -- The second statement will attach any remaining 1:1 links that have not got a corresponding upstream name.
                EXECUTE ('insert into '||_proc||' (name1, name2, link, identifier, startnode, endnode, length, geom) select distinct w.name1,w.name2,w.identifier,w.identifier,w.startnode,w.endnode,w.length,w.geom from '||_links||' w, '||_proc||' r, '||_nodes||' h where not exists (select 1 from '||_proc||' where link = w.identifier) and w.startnode = h.identifier and h.starts = 1 and h.ends = 1 and r.endnode = w.startnode and not exists (select n.name1 from pg_temp.names n where n.identifier = r.identifier and ((n.name1 IS NOT DISTINCT FROM w.name1) or (n.name1 IS DISTINCT FROM w.name1 and ((n.name1 = '''' or n.name1 IS NULL) or (w.name1 = '''' or w.name1 IS NULL)))))');
                
                -- It is reasonably assumed that changes of names in streams are usually confined to mergers and forks. As a result there may be minor inaccuracies where these changes do occur.
                
                EXECUTE ('select count(*) from '||_proc) into _subcounter;
                _subinserts_made := _subcounter - _subcheck;
                RAISE NOTICE 'Attached 1:1 links - % inserts made.',_subinserts_made;
            END LOOP;
            _subinserts_made := 1;
            
            EXECUTE ('select count(*) from '||_proc) into _check;
            
            -- Any time the 1:1 loop concludes the data is checked for (1:many or many:many) and many:1 links.
            -- First the 1(or many):many links are attached.
            
            -- The "valid" temporary table is cleared. This table holds the valid nodes to use in the next step, ensuring the following statements are effectively one step as opposed to four. Splitting the statements out resulted in greatly improved performance.
            TRUNCATE TABLE pg_temp.valid;
            EXECUTE ('INSERT INTO pg_temp.valid (identifier) select w.identifier from '||_links||' w, '||_proc||' r, '||_nodes||' h where not exists (select 1 from '||_proc||' where link = w.identifier) and w.startnode = h.identifier and h.starts > 1 and h.ends >= 1 and r.endnode = w.startnode and h.ends = (select count(endnode) from '||_proc||' where endnode = w.startnode group by endnode)');
            
            -- The initial query attaches named links to the accordingly named upstream link.
            EXECUTE ('insert into '||_proc||' (name1, name2, link, identifier, startnode, endnode, length, geom) select distinct w.name1,w.name2,v.identifier,r.identifier,w.startnode,w.endnode,w.length,w.geom from '||_links||' w, '||_proc||' r, pg_temp.valid v where v.identifier = w.identifier and r.endnode = w.startnode and w.identifier = (select identifier from '||_links||' where startnode = w.startnode order by length desc limit 1) and exists (select n.name1 from pg_temp.names n where n.identifier = r.identifier and ((n.name1 IS NOT DISTINCT FROM w.name1) or (n.name1 IS DISTINCT FROM w.name1 and ((n.name1 = '''' or n.name1 IS NULL) or (w.name1 = '''' or w.name1 IS NULL)))))');
            
            EXECUTE ('DELETE FROM pg_temp.valid v USING '||_proc||' r where r.link = v.identifier');
            
            -- Where no relevant upstream name is identified the process adds the named link starting a new stream ID.
            EXECUTE ('insert into '||_proc||' (name1, name2, link, identifier, startnode, endnode, length, geom) select distinct w.name1,w.name2,w.identifier,w.identifier,w.startnode,w.endnode,w.length,w.geom from '||_links||' w, '||_proc||' r, pg_temp.valid v where v.identifier = w.identifier and r.endnode = w.startnode and w.identifier = (select identifier from '||_links||' where startnode = w.startnode order by length desc limit 1) and not exists (select n.name1 from pg_temp.names n where n.identifier = r.identifier and ((n.name1 IS NOT DISTINCT FROM w.name1) or (n.name1 IS DISTINCT FROM w.name1 and ((n.name1 = '''' or n.name1 IS NULL) or (w.name1 = '''' or w.name1 IS NULL)))))');
            
            EXECUTE ('DELETE FROM pg_temp.valid v USING '||_proc||' r where r.link = v.identifier');
            
            -- Unnamed links are added to the longest named upstream river.
            EXECUTE ('insert into '||_proc||' (name1, name2, link, identifier, startnode, endnode, length, geom) select distinct w.name1,w.name2,w.identifier,r.identifier,w.startnode,w.endnode,w.length,w.geom from '||_links||' w, '||_proc||' r, pg_temp.valid v where v.identifier = w.identifier and r.endnode = w.startnode and w.name1 != '''' and w.name1 is not null and w.identifier != (select identifier from '||_links||' where startnode = w.startnode order by length desc limit 1) and exists (select n.name1 from pg_temp.names n where n.identifier = r.identifier and ((n.name1 IS NOT DISTINCT FROM w.name1) or (n.name1 IS DISTINCT FROM w.name1 and ((n.name1 = '''' or n.name1 IS NULL) or (w.name1 = '''' or w.name1 IS NULL)))))');
            
            EXECUTE ('DELETE FROM pg_temp.valid v USING '||_proc||' r where r.link = v.identifier');
            
            -- All remaining links are attached starting new stream IDs.
            EXECUTE ('insert into '||_proc||' (name1, name2, link, identifier, startnode, endnode, length, geom) select distinct w.name1,w.name2,w.identifier,w.identifier,w.startnode,w.endnode,w.length,w.geom from '||_links||' w, '||_proc||' r, pg_temp.valid v where v.identifier = w.identifier and r.endnode = w.startnode and w.name1 != '''' and w.name1 is not null and w.identifier != (select identifier from '||_links||' where startnode = w.startnode order by length desc limit 1) and not exists (select n.name1 from pg_temp.names n where n.identifier = r.identifier and ((n.name1 IS NOT DISTINCT FROM w.name1) or (n.name1 IS DISTINCT FROM w.name1 and ((n.name1 = '''' or n.name1 IS NULL) or (w.name1 = '''' or w.name1 IS NULL)))))');
            
            -- This concludes the 1(or many):many attachment for this processing round.
            EXECUTE ('select count(*) from '||_proc) into _counter;
            _inserts_made := _counter - _check;
            RAISE NOTICE 'Attached *|1:* links - % inserts made.',_inserts_made;
            _combined := _inserts_made;
            EXECUTE ('select count(*) from '||_proc) into _check;
            
            -- The following queries handle many:1 links. In most cases this would be a side stream joining a main stream. We need to identify the main stream and attach the new link accordingly.
            -- The "valid" temporary table is cleared. This table holds the valid nodes to use in the next step, ensuring the following statements are effectively one step as opposed to four. Splitting the statements out resulted in greatly improved performance.
            TRUNCATE TABLE pg_temp.valid;
            EXECUTE ('INSERT INTO pg_temp.valid (identifier) select w.identifier from '||_links||' w, '||_proc||' r, '||_nodes||' h where not exists (select 1 from '||_proc||' where link = w.identifier) and w.startnode = h.identifier and h.starts = 1 and h.ends > 1 and r.endnode = w.startnode and h.ends = (select count(endnode) from '||_proc||' where endnode = w.startnode group by endnode)');
            
            -- First we add all named links to the appropriately named upstream link where present.
            EXECUTE ('insert into '||_proc||' (link, name1, name2, identifier, startnode, endnode, length, geom) select distinct on (v.identifier) v.identifier,w.name1,w.name2,r.identifier,w.startnode,w.endnode,w.length,w.geom from '||_links||' w, '||_proc||' r, pg_temp.valid v where r.endnode = w.startnode and v.identifier = w.identifier and (select count(name1) from '||_proc||' where name1 = w.name1 and w.name1 is not null and w.name1 != '''' group by name1) > 1 and r.identifier = (select l.identifier from (select identifier, endnode from '||_proc||') as links, pg_temp.lengths l, pg_temp.names n where l.identifier = links.identifier and n.identifier = links.identifier and links.endnode = w.startnode and (n.name1 IS NOT DISTINCT FROM w.name1 and n.name1 is not null and n.name1 != '''') order by total desc limit 1)');
            
            EXECUTE ('DELETE FROM pg_temp.valid v USING '||_proc||' r where r.link = v.identifier');
            
            TRUNCATE TABLE pg_temp.filter;
            EXECUTE ('insert into pg_temp.filter (identifier) select sub.id from (select distinct on (l.endnode) l.identifier as id, l.total from '||_proc||' r, pg_temp.lengths l, '||_links||' w, pg_temp.valid v, pg_temp.names n where l.identifier = r.identifier and r.endnode = w.startnode and n.identifier = r.identifier and (n.name1 != '''' and n.name1 is not null) and w.identifier = v.identifier order by l.endnode, l.total desc) as sub');
            
            -- Next, we attach unnamed links to the longest named upstream river.
            EXECUTE ('insert into '||_proc||' (link, name1, name2, identifier, startnode, endnode, length, geom) select distinct on (v.identifier) v.identifier,w.name1,w.name2,r.identifier,w.startnode,w.endnode,w.length,w.geom from '||_links||' w, '||_proc||' r, pg_temp.valid v where r.endnode = w.startnode and v.identifier = w.identifier and r.identifier = (select l.identifier from (select identifier, endnode from '||_proc||') as links, pg_temp.lengths l, pg_temp.names n where l.identifier = links.identifier and n.identifier = links.identifier and links.endnode = w.startnode and (n.name1 IS DISTINCT FROM w.name1 and n.name1 is not null and n.name1 != '''') order by total desc limit 1)');
            
            EXECUTE ('DELETE FROM pg_temp.valid v USING '||_proc||' r where r.link = v.identifier');
            
            TRUNCATE TABLE pg_temp.filter;
            EXECUTE ('insert into pg_temp.filter (identifier) select sub.id from (select distinct on (l.endnode) l.identifier as id, l.total from '||_proc||' r, pg_temp.lengths l, '||_links||' w, pg_temp.valid v where l.identifier = r.identifier and r.endnode = w.startnode and w.identifier = v.identifier order by l.endnode, l.total desc) as sub');
            
            -- Next, we attach unnamed links to the longest unnamed upstream river. 
            EXECUTE ('insert into '||_proc||' (link, name1, name2, identifier, startnode, endnode, length, geom) select distinct on (v.identifier) v.identifier,w.name1,w.name2,r.identifier,w.startnode,w.endnode,w.length,w.geom from '||_links||' w, '||_proc||' r, pg_temp.valid v, pg_temp.names n where r.endnode = w.startnode and v.identifier = w.identifier and r.identifier in (select identifier from pg_temp.filter) and n.identifier = r.identifier and (w.name1 is null or w.name1 = '''') and (n.name1 is null or n.name1 = '''')');
            
            EXECUTE ('DELETE FROM pg_temp.valid v USING '||_proc||' r where r.link = v.identifier');
            
            -- Next, we attach named links to the longest identically named upstream link where there is more than one identically named upstream link.
            EXECUTE ('insert into '||_proc||' (link, name1, name2, identifier, startnode, endnode, length, geom) select distinct on (v.identifier) v.identifier,w.name1,w.name2,r.identifier,w.startnode,w.endnode,w.length,w.geom from '||_links||' w, '||_proc||' r, pg_temp.valid v, pg_temp.names n where r.endnode = w.startnode and v.identifier = w.identifier and r.identifier in (select identifier from pg_temp.filter) and n.identifier = r.identifier and n.name1 = w.name1');
            
            EXECUTE ('DELETE FROM pg_temp.valid v USING '||_proc||' r where r.link = v.identifier');
            
            -- Nearly there! Next, we attach any remaining named downstream links to the longest unnamed upstream link
            EXECUTE ('insert into '||_proc||' (link, name1, name2, identifier, startnode, endnode, length, geom) select distinct on (v.identifier) v.identifier,w.name1,w.name2,r.identifier,w.startnode,w.endnode,w.length,w.geom from '||_links||' w, '||_proc||' r, pg_temp.valid v, pg_temp.names n where r.endnode = w.startnode and v.identifier = w.identifier and r.identifier in (select identifier from pg_temp.filter) and n.identifier = r.identifier and (w.name1 is not null or w.name1 != '''') and (n.name1 is null or n.name1 = '''')');
            
            EXECUTE ('DELETE FROM pg_temp.valid v USING '||_proc||' r where r.link = v.identifier');
            
            -- Finally we add any remaining links, starting new river IDs.
            EXECUTE ('insert into '||_proc||' (link, name1, name2, identifier, startnode, endnode, length, geom) select distinct on (v.identifier) v.identifier,w.name1,w.name2,w.identifier,w.startnode,w.endnode,w.length,w.geom from '||_links||' w, pg_temp.valid v where v.identifier = w.identifier');
            
            EXECUTE ('select count(*) from '||_proc) into _counter;
            _inserts_made := _counter - _check;
            RAISE NOTICE 'Attached *:1 links - % inserts made.',_inserts_made;
            _combined := _combined + _inserts_made;
            IF _combined = 0 THEN
                
                -- If no other branches are being attached we insert all links where more links are started than ended where the end count and start count have not been reached and as a result the links have not been processed yet. In effect this creates new start points while ensuring complex branches are attached without corrupting the existing IDs.
                -- This is the reason we removed links earlier where start and end nodes were identical. While this is a way to handle slightly bigger versions of the same problem, the removed versions resulted in infinite loops within the processing.
                EXECUTE ('select count(*) from '||_proc) into _check;
                EXECUTE ('insert into '||_proc||' (link, name1, name2, identifier, startnode, endnode, length, geom) select distinct on (w.identifier) w.identifier,w.name1,w.name2,w.identifier,w.startnode,w.endnode,w.length,w.geom from '||_links||' w, '||_nodes||' h where h.starts > h.ends and w.startnode = h.identifier and not exists (select 1 from '||_proc||' where link = w.identifier)');
                EXECUTE ('select count(*) from '||_proc) into _counter;
                _inserts_made := _counter - _check;
                
                RAISE NOTICE 'Attached self closing loops - % inserts made.',_inserts_made;
                
                if _inserts_made <= 0 THEN
                    _inserts_made := -1;
                END IF;
                
            ELSE
                _inserts_made := _combined;
            END IF;
            RAISE NOTICE 'Total records in table: %',_counter;
        END LOOP;
        
        RAISE NOTICE 'Processing names.';
        
        -- As link names have not been changed in the previous steps to ensure consistency, we now update the link names to be consistent. While not strictly necessary for successful onward processing, this ensures the rivers resulting from any future merging are guaranteed to be named.
        EXECUTE ('UPDATE '||_proc||' set name1 = sub2.name1 FROM (select identifier, count(distinct name1) as c from '||_proc||' where name1 is not null and name1 != '''' group by identifier) as sub1, (select identifier, name1 from '||_proc||' where name1 is not null and name1 != '''' group by identifier, name1) as sub2 where sub1.identifier = sub2.identifier and sub1.c = 1 and '||_proc||'.identifier = sub1.identifier and '||_proc||'.identifier = sub2.identifier');  
        EXECUTE ('UPDATE '||_proc||' set name2 = sub2.name2 FROM (select identifier, count(distinct name2) as c from '||_proc||' where name2 is not null and name2 != '''' group by identifier) as sub1, (select identifier, name2 from '||_proc||' where name2 is not null and name2 != '''' group by identifier, name2) as sub2 where sub1.identifier = sub2.identifier and sub1.c = 1 and '||_proc||'.identifier = sub1.identifier and '||_proc||'.identifier = sub2.identifier');  
        EXECUTE ('UPDATE '||_proc||' set name1 = sub2.name1 FROM (select sub.identifier, max(sub.c) as m from (select identifier, count(name1) as c from '||_proc||' where name1 is not null and name1 != '''' group by identifier) as sub, '||_proc||' where '||_proc||'.name1 is not null and '||_proc||'.name1 != '''' group by sub.identifier) as sub1,(select identifier, name1 from '||_proc||' where name1 is not null and name1 != '''' group by identifier, name1) as sub2 where sub1.identifier = sub2.identifier and '||_proc||'.identifier = sub1.identifier and '||_proc||'.identifier = sub2.identifier');
        EXECUTE ('UPDATE '||_proc||' set name2 = sub2.name2 FROM (select sub.identifier, max(sub.c) as m from (select identifier, count(name2) as c from '||_proc||' where name2 is not null and name2 != '''' group by identifier) as sub, '||_proc||' where '||_proc||'.name2 is not null and '||_proc||'.name2 != '''' group by sub.identifier) as sub1,(select identifier, name2 from '||_proc||' where name2 is not null and name2 != '''' group by identifier, name2) as sub2 where sub1.identifier = sub2.identifier and '||_proc||'.identifier = sub1.identifier and '||_proc||'.identifier = sub2.identifier');
        
        RAISE NOTICE 'Names processed, joining data.';
        
        -- Now we merge streams by id, resulting in a cluster of named streams, though in places with different IDs despite identical names. This is a result of inconsistent naming in the source data and should only ever improve with subsequent source updates.
        EXECUTE ('DROP TABLE IF EXISTS '||_schema||'.inter');
        EXECUTE ('CREATE TABLE '||_schema||'.inter (gid serial, "name1" varchar(250), "name2" varchar(250), "identifier" varchar(38), "length" int)');
        PERFORM AddGeometryColumn(_schema,'inter','geom','0','MULTILINESTRING',2);
        PERFORM UpdateGeometrySRID(_schema,'inter','geom',27700);
        
        EXECUTE ('INSERT INTO '||_schema||'.inter (name1, name2, identifier, length, geom) select name1, name2, identifier, SUM(length), ST_Force_2D(ST_Multi(ST_Union(geom))) from '||_proc||' where name1 != '''' and name1 is not null group by identifier, name1, name2');
        
        RAISE NOTICE 'Raw processed data joined, sanitising output.';
        
        -- Finally we join all identically named streams in a certain area defined by our variable _buffer_radius together. We use a polygon buffer to create the overall covering polygon, then join all appropriate streams within into one definitive one.
        perform n.nspname ,c.relname FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace where n.nspname like 'pg_temp_%' AND pg_catalog.pg_table_is_visible(c.oid) AND Upper(relname) = Upper('collect1');
        IF FOUND THEN
            Drop table pg_temp.collect1;
        END IF;
        perform n.nspname ,c.relname FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace where n.nspname like 'pg_temp_%' AND pg_catalog.pg_table_is_visible(c.oid) AND Upper(relname) = Upper('buffer1');
        IF FOUND THEN
            Drop table pg_temp.buffer1;
        END IF;

        EXECUTE ('Drop table if exists '||_schema||'.singlepol');
        EXECUTE ('Drop table if exists '||_out);

        create temporary table pg_temp.collect1 (gid serial, name1 varchar(250), name2 varchar(250), geom geometry);
        create temporary table pg_temp.buffer1 (gid serial, name1 varchar(250), name2 varchar(250), geom geometry);
        EXECUTE ('create table '||_schema||'.singlepol (gid serial, name1 varchar(250), name2 varchar(250))');
        EXECUTE ('create table '||_out||' (gid serial, pid bigint, name1 varchar(250), name2 varchar(250), length bigint)');
        
        PERFORM AddGeometryColumn(_schema,'singlepol','geom','0','Polygon',2);
        PERFORM UpdateGeometrySRID(_schema,'singlepol','geom',27700);
        PERFORM AddGeometryColumn(_schema,_out_table,'geom','0','MULTILINESTRING',2);
        PERFORM UpdateGeometrySRID(_schema,_out_table,'geom',27700);
        
        DROP INDEX IF EXISTS pg_temp.c1_name1;
        DROP INDEX IF EXISTS pg_temp.c1_name2;
        DROP INDEX IF EXISTS pg_temp.b1_name1;
        DROP INDEX IF EXISTS pg_temp.b1_name2;
        EXECUTE ('DROP INDEX IF EXISTS '||_schema||'.s1_name1');
        EXECUTE ('DROP INDEX IF EXISTS '||_schema||'.s1_name2');
        CREATE INDEX c1_name1 on pg_temp.collect1 (name1);
        CREATE INDEX c1_name2 on pg_temp.collect1 (name2);
        CREATE INDEX b1_name1 on pg_temp.buffer1 (name1);
        CREATE INDEX b1_name2 on pg_temp.buffer1 (name2);
        EXECUTE ('CREATE INDEX s1_name1 on '||_schema||'.singlepol (name1)');
        EXECUTE ('CREATE INDEX s1_name2 on '||_schema||'.singlepol (name2)');

        EXECUTE ('insert into pg_temp.collect1 (name1, name2, geom) select r1.name1, r1.name2, st_linemerge(st_union(r1.geom)) as geom from '||_schema||'.inter r1 group by r1.name1, r1.name2');
        
        -- The following two statements are used solely for debugging and make little sense in normal operation. Crucially the collections and buffer count must be equal before splitting named rivers off.
        -- select count(*) into _counter from pg_temp.collect1;
        -- RAISE NOTICE '% collections in table.',_counter;
        
        EXECUTE ('insert into pg_temp.buffer1 (name1, name2, geom) select r1.name1, r1.name2, st_buffer(r1.geom,'||_buffer_radius||') as geom from pg_temp.collect1 r1');
        
        -- The following two statements are used solely for debugging and make little sense in normal operation. Crucially the collections and buffer count must be equal before splitting named rivers off.
        -- select count(*) into _counter from pg_temp.buffer1;
        -- RAISE NOTICE '% buffers in table.',_counter;
        
        EXECUTE ('insert into '||_schema||'.singlepol (name1, name2, geom) select r1.name1, r1.name2, ST_GeometryN(r1.geom, generate_series(1, ST_NumGeometries(r1.geom))) AS geom from pg_temp.buffer1 r1');
        
        -- These statements are commented as they are used solely for debugging. The count must be equal to the count raised in the final output notice.
        -- EXECUTE ('select count(*) from '||_schema||'.singlepol') into _counter;
        -- RAISE NOTICE '% processed rows in table.',_counter;
        
        EXECUTE ('ALTER TABLE '||_schema||'.inter DROP COLUMN IF EXISTS pid');
        EXECUTE ('ALTER TABLE '||_schema||'.inter ADD COLUMN pid bigint');
        
        EXECUTE ('Update '||_schema||'.inter set pid = pol.pid from (select f.gid as fid, s.gid as pid from '||_schema||'.inter f, '||_schema||'.singlepol s where s.name1 = f.name1 and ST_Within(f.geom, s.geom)) as pol where pol.fid = '||_schema||'.inter.gid');
        
        EXECUTE ('insert into '||_out||' (pid, name1,name2, length, geom) select pid, name1, name2, sum(length), st_union(geom) from '||_schema||'.inter group by pid, name1, name2');
        
        EXECUTE ('select count(*) from '||_out) into _counter;
        RAISE NOTICE 'Final output contains % individually named streams in the table.',_counter;
        
        -- If wanted we clean up the tables used in the processing.
        IF _clean = 1 THEN
            RAISE NOTICE 'Cleaning up working tables.';
            EXECUTE ('ALTER TABLE '||_nodes||' DROP COLUMN IF EXISTS starts');
            EXECUTE ('ALTER TABLE '||_nodes||' DROP COLUMN IF EXISTS ends');
            EXECUTE ('ALTER TABLE '||_nodes||' DROP COLUMN IF EXISTS nodecount');
            EXECUTE ('DROP TABLE '||_schema||'.inter');
            EXECUTE ('DROP TABLE pg_temp.collect1');
            EXECUTE ('DROP TABLE pg_temp.names');
            EXECUTE ('DROP TABLE pg_temp.lengths');
            EXECUTE ('DROP TABLE pg_temp.buffer1');
            EXECUTE ('DROP TABLE pg_temp.valid');
            EXECUTE ('DROP TABLE pg_temp.filter');
            EXECUTE ('DROP TABLE '||_schema||'.singlepol');
            EXECUTE ('DROP TABLE '||_proc);
        END IF;
        
        RAISE NOTICE 'Processing completed.';
       
    END IF;
    
END
$do$
