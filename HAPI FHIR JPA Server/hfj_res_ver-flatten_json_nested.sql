CREATE SCHEMA IF NOT EXISTS generated_views;
-- Generalized solution (for jsonb) https://stackoverflow.com/a/35179515
-- Create function "Create HAPI FHIR 'hfj_res_ver' grouped by 'res_type' and 'res_text_vc' type changed to 'jsonb' view"
CREATE OR REPLACE FUNCTION generated_views.create_json_flat_view (
    table_name TEXT,
    regular_columns TEXT,
    json_column TEXT,
    identifier_value TEXT,
    identifier_column TEXT
  ) RETURNS TEXT LANGUAGE plpgsql AS $$DECLARE cols TEXT;
BEGIN execute format (
  $ex$
  select string_agg(
      format('aravv.data->>%%1$L "%1$s.%%1$s"', key),
      ', '
    )
  from (
      select distinct key
      from generated_views."All_resources_all_versions_view" aravv,
        jsonb_each(data)
      WHERE aravv.data->>'resourceType' = %1$L
      order by 1
    ) s;
$ex$,
identifier_value
) into cols;
EXECUTE format(
  $ex$ DROP VIEW IF EXISTS generated_views."%4$s_all_versions_view";
CREATE VIEW generated_views."%4$s_all_versions_view" AS
SELECT %2$s,
  %6$s,
  CONCAT(%4$L, '/', hrv.res_id) AS "%4$s.referenceString"
FROM %1$s hrv
  RIGHT JOIN generated_views."All_resources_all_versions_view" aravv ON hrv.pid = aravv.pid
WHERE hrv.%5$s = %4$L $ex$,
  table_name,
  regular_columns,
  json_column,
  identifier_value,
  identifier_column,
  cols
);
RETURN 1;
END $$;
-- 
-- HAPI FHIR hfj_res_ver generate all FHIR resources views
CREATE OR REPLACE FUNCTION generated_views.generate_all_fhir_resources_views() RETURNS TEXT LANGUAGE plpgsql AS $$
DECLARE the_queries TEXT;
BEGIN EXECUTE format (
  $ex$
  CREATE OR REPLACE VIEW generated_views."All_resources_all_versions_view" AS --
-- https://stackoverflow.com/a/45592457
    with recursive flat (pid, key, value) as (
      select pid,
        key,
        value
      from public.hfj_res_ver hrv,
        jsonb_each(res_text_vc::jsonb)
      union
      select f.pid,
        concat(f.key, '.', j.key),
        j.value
      from flat f,
        jsonb_each(f.value) j
      where jsonb_typeof(f.value) = 'object'
    )
  select pid,
    jsonb_object_agg(key, value) as data
  from flat
  where jsonb_typeof(value) <> 'object'
  group by pid;
SELECT string_agg(
    format(
      'generated_views.create_json_flat_view(''public.hfj_res_ver'',''hrv.pid, partition_date, partition_id, res_deleted_at, res_version, has_tags, res_published, res_updated, res_encoding, res_text, res_id, res_type, res_ver'',''res_text_vc'',%%1$L,''res_type'')',
      "res_type_value"
    ),
    ', '
  )
FROM (
    SELECT DISTINCT all_fhir_resources_all_versions.res_type AS "res_type_value"
    FROM public."hfj_res_ver" all_fhir_resources_all_versions
    WHERE all_fhir_resources_all_versions.res_text_vc IS NOT NULL
    ORDER BY 1
  ) s;
$ex$
) INTO the_queries;
EXECUTE format(
  $ex$
  SELECT %1$s $ex$,
    the_queries
);
RETURN the_queries;
END $$;
SELECT generated_views.generate_all_fhir_resources_views();
