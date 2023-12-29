CREATE SCHEMA IF NOT EXISTS generated_views;
-- Generalized solution (for jsonb) https://stackoverflow.com/a/35179515
-- Create function "Create HAPI FHIR 'hfj_res_ver' grouped by 'res_type' and 'res_text_vc' type changed to 'jsonb' view"
CREATE OR REPLACE FUNCTION generated_views.create_json_flat_view (
    table_name TEXT,
    regular_columns TEXT,
    json_column TEXT,
    identifier_value TEXT,
    identifier_column TEXT
  ) RETURNS TEXT LANGUAGE plpgsql AS $$
DECLARE cols TEXT;
BEGIN EXECUTE format(
  $ex$
  CREATE OR REPLACE VIEW generated_views."z_flattened_%3$s_all_versions_view" AS with recursive flat (pid, key, value) as (
      select pid,
        key,
        value
      from %1$s hrv,
        jsonb_each("%2$s"::jsonb)
      where hrv."%4$s" = %3$L
      union
      select f.pid,
        concat(f.key, '.', j.key),
        j.value
      from flat f,
        jsonb_each(f.value) j
      where jsonb_typeof(f.value) = 'object'
    )
  select pid,
    jsonb_object_agg(key, value) as "%3$s"
  from flat
  where jsonb_typeof(value) <> 'object'
  group by pid;
$ex$,
table_name,
json_column,
identifier_value,
identifier_column
);
EXECUTE format (
  $ex$
  select string_agg(
      format('aravv."%1$s"->>%%1$L "%1$s.%%1$s"', key),
      ', '
    )
  from (
      select distinct key
      from generated_views."z_flattened_%1$s_all_versions_view" aravv,
        jsonb_each("%1$s")
      order by 1
    ) s;
$ex$,
identifier_value
) into cols;
execute format (
  $ex$
  -- DROP VIEW IF EXISTS generated_views."%4$s_all_versions_view" CASCADE;
  CREATE OR REPLACE VIEW generated_views."%4$s_all_versions_view" AS
  SELECT %2$s,
    CONCAT(%4$L, '/', 
      CASE
        WHEN (
          EXISTS (
            SELECT
              1
            FROM
              information_schema.columns
            WHERE
              table_name = 'hfj_resource'
              AND column_name = 'fhir_id'
          )
        )
        THEN (
          SELECT
            hr."fhir_id"
          FROM
            public.hfj_resource hr
          WHERE
            hr."res_id" = hrv."res_id"
        )
        ELSE 
          CASE
            WHEN (
              EXISTS (
                SELECT
                  1
                FROM
                  public.hfj_forced_id hfi
                WHERE
                  hfi."resource_pid" = hrv."res_id"
              )
            )
            THEN (
              SELECT
                hfi."forced_id"
              FROM
                public.hfj_forced_id hfi
              WHERE
                hfi."resource_pid" = hrv."res_id"
            )
            ELSE hrv.res_id::text
          END
      END
    ) AS "%4$s.referenceString",
    %6$s
  FROM %1$s hrv
    RIGHT JOIN generated_views."z_flattened_%4$s_all_versions_view" aravv ON hrv.pid = aravv.pid
  WHERE hrv.%5$s = %4$L;
$ex$,
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
