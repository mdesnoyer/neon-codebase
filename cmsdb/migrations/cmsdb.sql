--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;

--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';

CREATE USER pgadmin WITH SUPERUSER; 

SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: abstractintegration; Type: TABLE; Schema: public; Owner: pgadmin; Tablespace: 
--

CREATE TABLE abstractintegration (
    _data jsonb,
    _type character varying(128) NOT NULL
);


ALTER TABLE abstractintegration OWNER TO pgadmin;

--
-- Name: abstractplatform; Type: TABLE; Schema: public; Owner: pgadmin; Tablespace: 
--

CREATE TABLE abstractplatform (
    _data jsonb,
    _type character varying(128) NOT NULL
);


ALTER TABLE abstractplatform OWNER TO pgadmin;

--
-- Name: cdnhostingmetadatalist; Type: TABLE; Schema: public; Owner: pgadmin; Tablespace: 
--

CREATE TABLE cdnhostingmetadatalist (
    _data jsonb,
    _type character varying(128) NOT NULL
);


ALTER TABLE cdnhostingmetadatalist OWNER TO pgadmin;

--
-- Name: experimentstrategy; Type: TABLE; Schema: public; Owner: pgadmin; Tablespace: 
--

CREATE TABLE experimentstrategy (
    _data jsonb,
    _type character varying(128) NOT NULL
);


ALTER TABLE experimentstrategy OWNER TO pgadmin;

--
-- Name: neonapikey; Type: TABLE; Schema: public; Owner: pgadmin; Tablespace: 
--

CREATE TABLE neonapikey (
    _data jsonb,
    _type character varying(128) NOT NULL
);


ALTER TABLE neonapikey OWNER TO pgadmin;

--
-- Name: neonapirequest; Type: TABLE; Schema: public; Owner: pgadmin; Tablespace: 
--

CREATE TABLE neonapirequest (
    _data jsonb,
    _type character varying(128) NOT NULL
);


ALTER TABLE neonapirequest OWNER TO pgadmin;

--
-- Name: neonplatform; Type: TABLE; Schema: public; Owner: pgadmin; Tablespace: 
--

CREATE TABLE neonplatform (
    _data jsonb,
    _type character varying(128) NOT NULL
);


ALTER TABLE neonplatform OWNER TO pgadmin;

--
-- Name: neonuseraccount; Type: TABLE; Schema: public; Owner: pgadmin; Tablespace: 
--

CREATE TABLE neonuseraccount (
    _data jsonb,
    _type character varying(128) NOT NULL
);


ALTER TABLE neonuseraccount OWNER TO pgadmin;

--
-- Name: processingstrategy; Type: TABLE; Schema: public; Owner: pgadmin; Tablespace: 
--

CREATE TABLE processingstrategy (
    _data jsonb,
    _type character varying(128) NOT NULL
);


ALTER TABLE processingstrategy OWNER TO pgadmin;

--
-- Name: request; Type: TABLE; Schema: public; Owner: pgadmin; Tablespace: 
-- here for backwards compatibility, NeonApiRequest._baseclass_name() == request
--

CREATE TABLE request (
    _data jsonb,
    _type character varying(128) NOT NULL
);

--
-- Name: thumbnailmetadata; Type: TABLE; Schema: public; Owner: pgadmin; Tablespace: 
--

CREATE TABLE thumbnailmetadata (
    _data jsonb,
    _type character varying(128) NOT NULL
);


ALTER TABLE thumbnailmetadata OWNER TO pgadmin;

--
-- Name: thumbnailservingurls; Type: TABLE; Schema: public; Owner: pgadmin; Tablespace: 
--

CREATE TABLE thumbnailservingurls (
    _data jsonb,
    _type character varying(128) NOT NULL
);


ALTER TABLE thumbnailservingurls OWNER TO pgadmin;

--
-- Name: thumbnailstatus; Type: TABLE; Schema: public; Owner: pgadmin; Tablespace: 
--

CREATE TABLE thumbnailstatus (
    _data jsonb,
    _type character varying(128) NOT NULL
);


ALTER TABLE thumbnailstatus OWNER TO pgadmin;

--
-- Name: trackeraccountidmapper; Type: TABLE; Schema: public; Owner: pgadmin; Tablespace: 
--

CREATE TABLE trackeraccountidmapper (
    _data jsonb,
    _type character varying(128) NOT NULL
);


ALTER TABLE thumbnailstatus OWNER TO pgadmin;

--
-- Name: user; Type: TABLE; Schema: public; Owner: pgadmin; Tablespace: 
--

CREATE TABLE users (
    _data jsonb,
    _type character varying(128) NOT NULL
);


ALTER TABLE users OWNER TO pgadmin;

--
-- Name: videometadata; Type: TABLE; Schema: public; Owner: pgadmin; Tablespace: 
--

CREATE TABLE videometadata (
    _data jsonb,
    _type character varying(128) NOT NULL
);


ALTER TABLE videometadata OWNER TO pgadmin;

--
-- Name: videostatus; Type: TABLE; Schema: public; Owner: pgadmin; Tablespace: 
--

CREATE TABLE videostatus (
    _data jsonb,
    _type character varying(128) NOT NULL
);


ALTER TABLE videostatus OWNER TO pgadmin;

-- Data for Name: abstractintegration; Type: TABLE DATA; Schema: public; Owner: pgadmin
--

COPY abstractintegration (_data, _type) FROM stdin;
\.

-- Data for Name: abstractplatform; Type: TABLE DATA; Schema: public; Owner: pgadmin
--

COPY abstractplatform (_data, _type) FROM stdin;
\.


--
-- Data for Name: cdnhostingmetadatalist; Type: TABLE DATA; Schema: public; Owner: pgadmin
--

COPY cdnhostingmetadatalist (_data, _type) FROM stdin;
\.

--
-- Data for Name: experimentstrategy; Type: TABLE DATA; Schema: public; Owner: pgadmin
--

COPY experimentstrategy (_data, _type) FROM stdin;
\.


--
-- Data for Name: neonapikey; Type: TABLE DATA; Schema: public; Owner: pgadmin
--

COPY neonapikey (_data, _type) FROM stdin;
\.


--
-- Data for Name: neonapirequest; Type: TABLE DATA; Schema: public; Owner: pgadmin
--

COPY neonapirequest (_data, _type) FROM stdin;
\.


--
-- Data for Name: neonuseraccount; Type: TABLE DATA; Schema: public; Owner: pgadmin
--

COPY neonuseraccount (_data, _type) FROM stdin;
\.

--
-- Data for Name: processingstrategy; Type: TABLE DATA; Schema: public; Owner: pgadmin
--

COPY processingstrategy (_data, _type) FROM stdin;
\.

--
-- Data for Name: request; Type: TABLE DATA; Schema: public; Owner: pgadmin
--

COPY request (_data, _type) FROM stdin;
\.

--
-- Data for Name: thumbnailmetadata; Type: TABLE DATA; Schema: public; Owner: pgadmin
--

COPY thumbnailmetadata (_data, _type) FROM stdin;
\.


--
-- Data for Name: thumbnailservingurls; Type: TABLE DATA; Schema: public; Owner: pgadmin
--

COPY thumbnailservingurls (_data, _type) FROM stdin;
\.


--
-- Data for Name: thumbnailstatus; Type: TABLE DATA; Schema: public; Owner: pgadmin
--

COPY thumbnailstatus (_data, _type) FROM stdin;
\.

--
-- Data for Name: trackeraccountidmapper; Type: TABLE DATA; Schema: public; Owner: pgadmin
--

COPY trackeraccountidmapper (_data, _type) FROM stdin;
\.

--
-- Data for Name: users; Type: TABLE DATA; Schema: public; Owner: pgadmin
--

COPY users (_data, _type) FROM stdin;
\.


--
-- Data for Name: videometadata; Type: TABLE DATA; Schema: public; Owner: pgadmin
--

COPY videometadata (_data, _type) FROM stdin;
\.


--
-- Data for Name: videostatus; Type: TABLE DATA; Schema: public; Owner: pgadmin
--

COPY videostatus (_data, _type) FROM stdin;
\.


CREATE UNIQUE INDEX abstractplatform_key ON abstractplatform USING btree (((_data ->> 'key'::text)));
CREATE UNIQUE INDEX abstractintegration_key ON abstractintegration USING btree (((_data ->> 'key'::text)));
CREATE UNIQUE INDEX experimentstrategy_key ON experimentstrategy USING btree (((_data ->> 'key'::text)));
CREATE UNIQUE INDEX neonapirequest_key ON neonapirequest USING btree (((_data ->> 'key'::text)));
CREATE UNIQUE INDEX neonuseraccount_key ON neonuseraccount USING btree (((_data ->> 'key'::text)));
CREATE UNIQUE INDEX processingstrategy_key ON processingstrategy USING btree (((_data ->> 'key'::text)));
CREATE UNIQUE INDEX request_key ON request USING btree (((_data ->> 'key'::text)));
CREATE UNIQUE INDEX thumbnailmetadata_key ON thumbnailmetadata USING btree (((_data ->> 'key'::text)));
CREATE UNIQUE INDEX videometadata_key ON videometadata USING btree (((_data ->> 'key'::text)));
CREATE UNIQUE INDEX thumbnailservingurls_key ON thumbnailservingurls USING btree (((_data ->> 'key'::text)));

--
-- Name: public; Type: ACL; Schema: -; Owner: pgadmin
--

CREATE OR REPLACE FUNCTION tables_notify_func() RETURNS trigger as $$
DECLARE
  payload text;
BEGIN   
    IF TG_OP = 'DELETE' THEN
    payload := row_to_json(tmp)::text FROM (
            SELECT
                OLD.*,
                TG_OP
        ) tmp;
    ELSE
        payload := row_to_json(tmp)::text FROM (
            SELECT 
                NEW.*,
                TG_OP
        ) tmp;
    END IF;

  PERFORM pg_notify(TG_TABLE_NAME::text, payload);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER abstractplatform_notify_trig
AFTER INSERT OR UPDATE OR DELETE
ON abstractplatform
FOR EACH ROW EXECUTE PROCEDURE tables_notify_func();

CREATE TRIGGER abstractintegration_notify_trig
AFTER INSERT OR UPDATE OR DELETE
ON abstractintegration
FOR EACH ROW EXECUTE PROCEDURE tables_notify_func();

CREATE TRIGGER cdnhostingmetadatalist_notify_trig
AFTER INSERT OR UPDATE OR DELETE
ON cdnhostingmetadatalist
FOR EACH ROW EXECUTE PROCEDURE tables_notify_func();

CREATE TRIGGER experimentstrategy_notify_trig
AFTER INSERT OR UPDATE OR DELETE
ON experimentstrategy
FOR EACH ROW EXECUTE PROCEDURE tables_notify_func();

CREATE TRIGGER neonapikey_notify_trig
AFTER INSERT OR UPDATE OR DELETE
ON neonapikey
FOR EACH ROW EXECUTE PROCEDURE tables_notify_func();

CREATE TRIGGER neonapirequest_notify_trig
AFTER INSERT OR UPDATE OR DELETE
ON neonapirequest
FOR EACH ROW EXECUTE PROCEDURE tables_notify_func();

CREATE TRIGGER neonplatform_notify_trig
AFTER INSERT OR UPDATE OR DELETE
ON neonplatform
FOR EACH ROW EXECUTE PROCEDURE tables_notify_func();

CREATE TRIGGER neonuseraccount_notify_trig
AFTER INSERT OR UPDATE OR DELETE
ON neonuseraccount
FOR EACH ROW EXECUTE PROCEDURE tables_notify_func();

CREATE TRIGGER processingstrategy_notify_trig
AFTER INSERT OR UPDATE OR DELETE
ON processingstrategy
FOR EACH ROW EXECUTE PROCEDURE tables_notify_func();

CREATE TRIGGER request_notify_trig
AFTER INSERT OR UPDATE OR DELETE
ON request
FOR EACH ROW EXECUTE PROCEDURE tables_notify_func();

CREATE TRIGGER thumbnailmetadata_notify_trig
AFTER INSERT OR UPDATE OR DELETE
ON thumbnailmetadata
FOR EACH ROW EXECUTE PROCEDURE tables_notify_func();

CREATE TRIGGER thumbnailservingurls_notify_trig
AFTER INSERT OR UPDATE OR DELETE
ON thumbnailservingurls
FOR EACH ROW EXECUTE PROCEDURE tables_notify_func();

CREATE TRIGGER thumbnailstatus_notify_trig
AFTER INSERT OR UPDATE OR DELETE
ON thumbnailstatus
FOR EACH ROW EXECUTE PROCEDURE tables_notify_func();

CREATE TRIGGER trackeraccountidmapper_notify_trig
AFTER INSERT OR UPDATE OR DELETE
ON trackeraccountidmapper
FOR EACH ROW EXECUTE PROCEDURE tables_notify_func();

CREATE TRIGGER users_notify_trig
AFTER INSERT OR UPDATE OR DELETE
ON users
FOR EACH ROW EXECUTE PROCEDURE tables_notify_func();

CREATE TRIGGER videometadata_notify_trig
AFTER INSERT OR UPDATE OR DELETE
ON videometadata
FOR EACH ROW EXECUTE PROCEDURE tables_notify_func();

CREATE TRIGGER videostatus_notify_trig
AFTER INSERT OR UPDATE OR DELETE
ON videostatus
FOR EACH ROW EXECUTE PROCEDURE tables_notify_func();

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM pgadmin;
GRANT ALL ON SCHEMA public TO pgadmin;
GRANT ALL ON SCHEMA public TO PUBLIC;

--
-- PostgreSQL database dump complete
--
