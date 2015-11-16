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


SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: brightcoveintegration; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE brightcoveplatform (
    _data jsonb,
    _type character varying(128) NOT NULL
);


ALTER TABLE brightcoveplatform OWNER TO postgres;

--
-- Name: abstractintegration; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE abstractintegration (
    _data jsonb,
    _type character varying(128) NOT NULL
);


ALTER TABLE abstractintegration OWNER TO postgres;

--
-- Name: abstractplatform; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE abstractplatform (
    _data jsonb,
    _type character varying(128) NOT NULL
);


ALTER TABLE abstractplatform OWNER TO postgres;

--
-- Name: cdnhostingmetadatalist; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE cdnhostingmetadatalist (
    _data jsonb,
    _type character varying(128) NOT NULL
);


ALTER TABLE cdnhostingmetadatalist OWNER TO postgres;

--
-- Name: experimentstrategy; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE experimentstrategy (
    _data jsonb,
    _type character varying(128) NOT NULL
);


ALTER TABLE experimentstrategy OWNER TO postgres;

--
-- Name: neonapikey; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE neonapikey (
    _data jsonb,
    _type character varying(128) NOT NULL
);


ALTER TABLE neonapikey OWNER TO postgres;

--
-- Name: neonapirequest; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE neonapirequest (
    _data jsonb,
    _type character varying(128) NOT NULL
);


ALTER TABLE neonapirequest OWNER TO postgres;

--
-- Name: neonplatform; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE neonplatform (
    _data jsonb,
    _type character varying(128) NOT NULL
);


ALTER TABLE neonplatform OWNER TO postgres;

--
-- Name: neonuseraccount; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE neonuseraccount (
    _data jsonb,
    _type character varying(128) NOT NULL
);


ALTER TABLE neonuseraccount OWNER TO postgres;

--
-- Name: ooyalaintegration; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE ooyalaintegration (
    _data jsonb,
    _type character varying(128) NOT NULL
);


ALTER TABLE ooyalaintegration OWNER TO postgres;

--
-- Name: thumbnailmetadata; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE thumbnailmetadata (
    _data jsonb,
    _type character varying(128) NOT NULL
);


ALTER TABLE thumbnailmetadata OWNER TO postgres;

--
-- Name: thumbnailservingurls; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE thumbnailservingurls (
    _data jsonb,
    _type character varying(128) NOT NULL
);


ALTER TABLE thumbnailservingurls OWNER TO postgres;

--
-- Name: thumbnailstatus; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE thumbnailstatus (
    _data jsonb,
    _type character varying(128) NOT NULL
);


ALTER TABLE thumbnailstatus OWNER TO postgres;

--
-- Name: thumbnailurlmapper; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE thumbnailurlmapper (
    _data jsonb,
    _type character varying(128) NOT NULL
);


ALTER TABLE thumbnailurlmapper OWNER TO postgres;

--
-- Name: videometadata; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE videometadata (
    _data jsonb,
    _type character varying(128) NOT NULL
);


ALTER TABLE videometadata OWNER TO postgres;

--
-- Name: videostatus; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE videostatus (
    _data jsonb,
    _type character varying(128) NOT NULL
);


ALTER TABLE videostatus OWNER TO postgres;

-- Data for Name: abstractintegration; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY abstractintegration (_data, _type) FROM stdin;
\.

-- Data for Name: abstractplatform; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY abstractplatform (_data, _type) FROM stdin;
\.

--
-- Data for Name: brightcoveplatform; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY brightcoveplatform (_data, _type) FROM stdin;
\.


--
-- Data for Name: cdnhostingmetadatalist; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY cdnhostingmetadatalist (_data, _type) FROM stdin;
\.


--
-- Data for Name: experimentstrategy; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY experimentstrategy (_data, _type) FROM stdin;
\.


--
-- Data for Name: neonapikey; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY neonapikey (_data, _type) FROM stdin;
\.


--
-- Data for Name: neonapirequest; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY neonapirequest (_data, _type) FROM stdin;
\.


--
-- Data for Name: neonplatform; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY neonplatform (_data, _type) FROM stdin;
\.


--
-- Data for Name: neonuseraccount; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY neonuseraccount (_data, _type) FROM stdin;
\.


--
-- Data for Name: ooyalaintegration; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY ooyalaintegration (_data, _type) FROM stdin;
\.


--
-- Data for Name: thumbnailmetadata; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY thumbnailmetadata (_data, _type) FROM stdin;
\.


--
-- Data for Name: thumbnailservingurls; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY thumbnailservingurls (_data, _type) FROM stdin;
\.


--
-- Data for Name: thumbnailstatus; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY thumbnailstatus (_data, _type) FROM stdin;
\.


--
-- Data for Name: thumbnailurlmapper; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY thumbnailurlmapper (_data, _type) FROM stdin;
\.


--
-- Data for Name: videometadata; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY videometadata (_data, _type) FROM stdin;
\.


--
-- Data for Name: videostatus; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY videostatus (_data, _type) FROM stdin;
\.


--
-- Name: account_id; Type: INDEX; Schema: public; Owner: postgres; Tablespace: 
--

CREATE UNIQUE INDEX account_id ON neonuseraccount USING btree (((_data ->> 'account_id'::text)));


--
-- Name: public; Type: ACL; Schema: -; Owner: postgres
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

CREATE TRIGGER neonuseraccount_notify_trig
AFTER INSERT OR UPDATE OR DELETE
ON neonuseraccount
FOR EACH ROW EXECUTE PROCEDURE tables_notify_func();

CREATE TRIGGER neonapikey_notify_trig
AFTER INSERT OR UPDATE OR DELETE
ON neonapikey
FOR EACH ROW EXECUTE PROCEDURE tables_notify_func();

CREATE TRIGGER abstractplatform_notify_trig
AFTER INSERT OR UPDATE OR DELETE
ON abstractplatform
FOR EACH ROW EXECUTE PROCEDURE tables_notify_func();

CREATE TRIGGER abstractintegration_notify_trig
AFTER INSERT OR UPDATE OR DELETE
ON abstractintegration
FOR EACH ROW EXECUTE PROCEDURE tables_notify_func();

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM postgres;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- PostgreSQL database dump complete
--

